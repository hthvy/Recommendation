import os
from elasticsearch import Elasticsearch
from datetime import datetime

# 1. Kết nối với Server Elasticsearch (Dùng biến môi trường trong Docker hoặc localhost)
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
es = Elasticsearch(ES_HOST)
INDEX_NAME = "conferences_index"

# =====================================================================
# TÌM ỨNG VIÊN THEO TOPIC (Phục vụ Kịch bản 1, 3, 4)
# =====================================================================
def get_upcoming_candidates_by_topics(topics, limit=100):
    today = datetime.utcnow().isoformat()
    """
    Lấy ứng viên khớp Topic, sử dụng 'match' để không phân biệt hoa thường.
    """
    # KỊCH BẢN 4: Trending chung
    if not topics:
        query = {
            "size": limit,
            "query": {
                "bool": {
                    "must": [
                        { "match_all": {} }
                    ],
                    "filter": [
                        {
                            "range": {
                                "latestSubmissionDate": {
                                    "gt": today
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [{"view_count": {"order": "desc"}}]
        }
    # KỊCH BẢN 1 & 3: Có Topic
    else:
        should_clauses = []

        for item in topics:
            # Hỗ trợ cả 2 kiểu:
            # 1) "security"
            # 2) ("security", 15.9)
            if isinstance(item, (list, tuple)) and len(item) == 2:
                topic, score = item
                try:
                    boost = float(score)
                except Exception:
                    boost = 1.0
            else:
                topic = item
                boost = 1.0

            should_clauses.append({
                "match": {
                    "topics": {
                        "query": topic,
                        "boost": boost
                    }
                }
            })
        
        query = {
            "size": limit,
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1,
                    "filter": [
                        {
                            "range": {
                                "latestSubmissionDate": {
                                    "gt": today
                                }
                            }
                        }
                    ]
                }
            }
        }
    
    try:
        # Debug: In ra query để kiểm tra nếu cần
        # print(json.dumps(query, indent=2)) 
        
        response = es.search(index=INDEX_NAME, body=query)
        hits = response['hits']['hits']
        
        # Thêm log để bạn biết ES đã thực sự tìm thấy đồ
        print(f"      [ES-RESULT] Tìm thấy {len(hits)} hội nghị khớp.")
        
        return [
            {
                "id": str(hit['_source']['id']),
                "view_count": int(hit['_source'].get('view_count', 0))
            }
            for hit in hits
        ]
    except Exception as e:
        print(f"Lỗi truy vấn ES (Candidate by Topic): {e}")
        return []
    
# =====================================================================
# HÀM DÀNH CHO NGƯỜI 2: TÌM HỘI NGHỊ TƯƠNG ĐỒNG (Phục vụ Kịch bản 5)
# =====================================================================
def get_similar_conferences(conf_id, limit=100):
    """
    Tìm các hội nghị có nội dung (title, topics) tương tự với hội nghị đang xem.
    """
    today = datetime.utcnow().isoformat()
    
    query = {
        "size": limit,
        "query": {
            "bool": {
                "must": [
                    {
                        "more_like_this": {
                            "fields": ["title", "topics"],
                            "like": [{"_id": conf_id}],
                            "min_term_freq": 1,
                            "max_query_terms": 12
                        }
                    }
                ],
                #"filter": [
                    #{"range": {"endDate": {"gte": today}}} # CỨNG: Chưa hết hạn
                #]
            }
        }
    }

    try:
        response = es.search(index=INDEX_NAME, body=query)
        hits = response['hits']['hits']
        return [str(hit['_source']['id']) for hit in hits if hit.get('_source', {}).get('id')]
    except Exception as e:
        print(f"Lỗi truy vấn ES (Similar Conferences): {e}")
        return []

# hàm thuật toán liên quan search
def search_conferences_by_heuristic(detected_keyword, keyword_type="KEYWORD", limit=50):
    """
    Tìm kiếm hội nghị và trả về Điểm Heuristic (ES Score).
    keyword_type: TITLE, ACRONYM, KEYWORD để ES biết nên ưu tiên tìm ở field nào.
    """
    # Điều chỉnh trọng số tùy theo loại từ khóa mà AI nội bộ detect được
    fields_mapping = {
        "TITLE": ["title^4", "acronym", "topics"],
        "ACRONYM": ["acronym^4", "title", "topics"],
        "KEYWORD": ["topics^4", "title^2", "acronym"]
    }
    search_fields = fields_mapping.get(keyword_type.upper(), ["title^2", "topics^2"])

    query = {
        "size": limit,
        "query": {
            "multi_match": {
                "query": detected_keyword,
                "fields": search_fields,
                "type": "best_fields"
            }
        }
    }
    
    try:
        from core.es_client import es, INDEX_NAME # Import biến môi trường hiện tại của bạn
        response = es.search(index=INDEX_NAME, body=query)
        hits = response['hits']['hits']
        
        # Trả về: { "conf_id_1": 4.5, "conf_id_2": 2.1 }
        return {str(hit['_source']['id']): float(hit['_score']) for hit in hits}
    except Exception as e:
        print(f"[ES_ERROR] Lỗi lấy Heuristic từ ES: {e}")
        return {}
    
    try:
        response = es.search(index=INDEX_NAME, body=query)
        hits = response['hits']['hits']
        return [hit['_source']['id'] for hit in hits]
    except Exception as e:
        print(f"Lỗi truy vấn ES (Similar Conferences): {e}")
        return []
