import os
import requests
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from core.db_client import get_mongodb_connection
from dotenv import load_dotenv # 1. IMPORT THƯ VIỆN ĐỌC .ENV

load_dotenv()
# 1. Cấu hình
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
NESTJS_API_URL = os.getenv(
    "NESTJS_API_URL",
    "http://localhost:3000/api/v1/conference/all"
)

es = Elasticsearch(ES_HOST)
INDEX_NAME = "conferences_index"

def parse_date(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def extract_submission_dates(conf):
    """
    Lấy tất cả submission dates từ API response.
    Ưu tiên toDate vì đây thường là deadline submission.
    Nếu toDate không có thì fallback sang fromDate.
    """
    raw_dates = conf.get("submissionDates") or []
    dates = []

    for d in raw_dates:
        raw = d.get("toDate") or d.get("fromDate")
        parsed = parse_date(raw)

        if parsed:
            dates.append(parsed)

    return dates


def build_view_count_pipeline(days=None):
    match_stage = {
        "event_type": {"$in": ["CLICK_CONFERENCE_TITLE", "CLICK_RELATED_DETAIL"]},
        "metadata.conference_id": {"$exists": True, "$ne": None}
    }

    if days is not None:
        cutoff_dt = datetime.utcnow() - timedelta(days=days)
        cutoff_iso = cutoff_dt.isoformat()
        match_stage["$or"] = [
            {"timestamp": {"$gte": cutoff_dt}},
            {"timestamp": {"$gte": cutoff_iso}}
        ]

    return [
        {"$match": match_stage},
        {
            "$group": {
                "_id": "$metadata.conference_id",
                "count": {"$sum": 1}
            }
        }
    ]


def aggregate_view_counts(collection, days=None):
    results = collection.aggregate(build_view_count_pipeline(days))

    view_count_map = {}
    for row in results:
        conf_id = str(row["_id"])
        count = int(row["count"])
        view_count_map[conf_id] = count

    return view_count_map


def get_view_counts_from_mongo():
    """
    Count conference views from Mongo logs for trending.
    Prefer 7 days, then 30 days, then all-time as fallback.
    """
    db = get_mongodb_connection()
    collection = db["raw_user_logs"]
    # Trending should reflect recent interest first, then gracefully fall back.
    fallback_windows = [
        ("7d", 7),
        ("30d_fallback", 30),
        ("all_time_fallback", None)
    ]

    for label, days in fallback_windows:
        view_count_map = aggregate_view_counts(collection, days)

        if view_count_map:
            print(f"Counted view_count from Mongo for {len(view_count_map)} conferences. window={label}")
            return view_count_map

        print(f"No view_count found in Mongo. window={label}")

    return {}

def sync_data_to_es():
    print(f"Bắt đầu gọi API Backend NestJS: {NESTJS_API_URL}")
    
    try:
        # =====================================================================
        # ✅ BƯỚC 2 (ĐÃ SỬA): Truyền tham số qua params giúp băm URL chuẩn 100%
        # =====================================================================
        query_params = {"limit": 1000}
        response = requests.get(NESTJS_API_URL, params=query_params, timeout=10)
        response.raise_for_status() 
        api_response = response.json()
        
        # 2. Rút trích mảng hội nghị từ biến 'payload'
        if isinstance(api_response, dict) and "payload" in api_response:
            conferences = api_response["payload"]
            print(f"Đã tìm thấy {len(conferences)} hội nghị trong API!")
        else:
            print("Lỗi: Không tìm thấy key 'payload' trong kết quả trả về.")
            return
            
    except Exception as e:
        print(f"Không thể lấy dữ liệu từ API tại {NESTJS_API_URL}. Lỗi: {e}")
        print("💡 Gợi ý: Hãy chắc chắn rằng NestJS local đang chạy bằng lệnh 'npm run start:dev' tại cổng 3000!")
        return

    if not conferences:
        print("Cảnh báo: Danh sách hội nghị trống.")
        return
    
    # Lấy view_count thật từ Mongo
    view_count_map = get_view_counts_from_mongo()

    # 3. Xóa Index cũ và tạo Mapping mới cho Elasticsearch
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text"},
                "topics": {"type": "text"},
                "endDate": {"type": "date"},
                "submissionDates": {"type": "date"},
                "latestSubmissionDate": {"type": "date"},
                "view_count": {"type": "integer"}
            }
        }
    }
    es.indices.create(index=INDEX_NAME, body=mapping)
    
    # 4. Map dữ liệu từ JSON chuẩn của Backend vào ES
    actions = []
    for conf in conferences:
        # Lấy ngày kết thúc hội nghị từ object 'dates' nếu có
        end_date = None
        if "dates" in conf and isinstance(conf["dates"], dict):
            end_date = conf["dates"].get("toDate")

        # Lấy các deadline submission
        submission_dates = extract_submission_dates(conf)
        submission_dates_iso = [d.isoformat() for d in submission_dates]
        latest_submission_date = max(submission_dates).isoformat() if submission_dates else None

        doc = {
            "_index": INDEX_NAME,
            "_id": str(conf.get("id")),
            "_source": {
                "id": str(conf.get("id")),
                "title": conf.get("title", ""),
                "endDate": end_date,
                "topics": conf.get("topics", []),
                "view_count": view_count_map.get(str(conf.get("id")), 0),

                # Dùng để lọc conference còn hạn submission
                "submissionDates": submission_dates_iso,
                "latestSubmissionDate": latest_submission_date,
            }
        }
        actions.append(doc)
        
    # 5. Đẩy hàng loạt vào Elasticsearch
    if actions:
        bulk(es, actions)
        es.indices.refresh(index=INDEX_NAME)
        print(f"Đã đồng bộ THÀNH CÔNG {len(actions)} hội nghị vào Elasticsearch!")

if __name__ == "__main__":
    sync_data_to_es()
