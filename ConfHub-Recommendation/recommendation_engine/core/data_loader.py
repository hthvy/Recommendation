import os
import pandas as pd
from pymongo import MongoClient
from sklearn.metrics.pairwise import cosine_similarity

from core.recommend_config import get_recommend_config, get_short_term_event_weights

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")


# =========================================================
# TRỌNG SỐ CHO MA TRẬN RATING
# =========================================================
EVENT_WEIGHTS = {
    "VIEW_CONFERENCE_DETAIL": 1.8,
    "CLICK_CONFERENCE_TITLE": 2.0,
    "CLICK_RELATED_DETAIL": 2.0,
    "CLICK_EXTERNAL_WEBSITE": 2.5,
    "CLICK_FOLLOW_STAR": 3.0,     # FOLLOW là dương, UNFOLLOW xử lý riêng
    "CLICK_ADD_CALENDAR": 2.8,    # ADD là dương, REMOVE xử lý riêng
    "SUBMIT_FEEDBACK": 0.0,       # xử lý riêng theo rating
    "TIME_ON_PAGE": 1.2,          # xử lý riêng theo seconds nếu có
    "SCROLL_DEPTH": 1.0,          # xử lý riêng theo percent nếu có
    "SEARCH_CONFERENCE": 0.5,     # search chỉ nên đóng góp nhẹ vào rating item
}

NEGATIVE_ACTIONS = {
    ("CLICK_FOLLOW_STAR", "UNFOLLOW"),
    ("CLICK_ADD_CALENDAR", "REMOVE"),
    ("CLICK_ADD_CALENDAR", "REMOVE_CALENDAR"),
}

POSITIVE_ACTIONS = {
    ("CLICK_FOLLOW_STAR", "FOLLOW"),
    ("CLICK_ADD_CALENDAR", "ADD"),
    ("CLICK_ADD_CALENDAR", "ADD_CALENDAR"),
}


# =========================================================
# HELPERS
# =========================================================
def _score_feedback(metadata, base_weight=2.0):
    rating = metadata.get("rating_score") or metadata.get("rating")
    try:
        rating = int(rating)
    except Exception:
        rating = 3

    if rating >= 4:
        return abs(base_weight)
    if rating == 3:
        return abs(base_weight) * 0.5
    return -abs(base_weight)


def _score_time_on_page(metadata, base_weight=1.2):
    seconds = (
        metadata.get("seconds")
        or metadata.get("duration")
        or metadata.get("time_spent")
        or 0
    )
    try:
        seconds = float(seconds)
    except Exception:
        seconds = 0.0

    if seconds >= 180:
        return base_weight
    if seconds >= 90:
        return base_weight * 0.75
    if seconds >= 30:
        return base_weight * (5.0 / 12.0)
    return 0.0


def _score_scroll_depth(metadata, base_weight=1.0):
    percent = (
        metadata.get("depth_percent")
        or metadata.get("scroll_percent")
        or metadata.get("percent")
        or 0
    )
    try:
        percent = float(percent)
    except Exception:
        percent = 0.0

    if percent >= 100:
        return base_weight
    if percent >= 75:
        return base_weight * 0.8
    if percent >= 50:
        return base_weight * 0.5
    if percent >= 25:
        return base_weight * 0.2
    return 0.0


def _compute_event_score(doc, event_weights=None):
    event_type = doc.get("event_type")
    metadata = doc.get("metadata", {}) or {}
    action = (metadata.get("action") or "").upper()

    weights = event_weights or EVENT_WEIGHTS
    base_score = weights.get(event_type, 0.0)

    if event_type == "SUBMIT_FEEDBACK":
        return _score_feedback(metadata, base_score)

    if event_type == "TIME_ON_PAGE":
        return _score_time_on_page(metadata, base_score)

    if event_type == "SCROLL_DEPTH":
        return _score_scroll_depth(metadata, base_score)

    if (event_type, action) in NEGATIVE_ACTIONS:
        return -abs(base_score)

    if (event_type, action) in POSITIVE_ACTIONS:
        return abs(base_score)

    return base_score


# =========================================================
# MAIN
# =========================================================
def load_unified_matrices():
    print("⏳ [DATA LOADER] Kéo dữ liệu log để tạo tài nguyên lõi...")
    config = get_recommend_config()
    event_weights = get_short_term_event_weights(config)
    client = MongoClient(MONGO_URI)
    collection = client["thesis_db"]["raw_user_logs"]

    # Chỉ lấy log có user thật và có conference_id
    cursor = collection.find({
        "user_id": {"$exists": True, "$ne": "guest"},
        "metadata.conference_id": {"$exists": True}
    })

    raw_data = []

    for doc in cursor:
        user_id = doc.get("user_id")
        metadata = doc.get("metadata", {}) or {}
        conf_id = metadata.get("conference_id")

        if not user_id or not conf_id:
            continue

        score = _compute_event_score(doc, event_weights)

        if score != 0:
            raw_data.append({
                "user_id": str(user_id),
                "conf_id": str(conf_id),
                "score": score
            })

    client.close()

    if not raw_data:
        print("⚠️ Không có đủ dữ liệu log.")
        return None, None

    df = pd.DataFrame(raw_data)

    # Cộng dồn điểm nếu user có nhiều tương tác với cùng 1 conference
    df_grouped = df.groupby(["user_id", "conf_id"])["score"].sum().reset_index()

    # Có thể chặn score để tránh 1 user spam event làm méo ma trận
    df_grouped["score"] = df_grouped["score"].clip(lower=-10, upper=10)

    # 1. TẠO MA TRẬN GỐC R (User x Item)
    user_item_matrix = df_grouped.pivot(
        index="user_id",
        columns="conf_id",
        values="score"
    ).fillna(0)

    # 2. TÍNH USER-USER SIMILARITY
    user_sim_matrix = pd.DataFrame(
        cosine_similarity(user_item_matrix),
        index=user_item_matrix.index,
        columns=user_item_matrix.index
    )

    print(f"✅ Đã tạo xong Ma trận gốc {user_item_matrix.shape} và 2 Ma trận Similarity trên RAM.")
    return user_item_matrix, user_sim_matrix
