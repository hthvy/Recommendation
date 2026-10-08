import json
from collections import defaultdict
from datetime import datetime, timedelta

from core.db_client import get_postgres_connection, get_mongodb_connection, get_redis_connection
from core.es_client import get_upcoming_candidates_by_topics
from core.recommend_config import (
    DEFAULT_RECOMMEND_CONFIG,
    get_recommend_config,
    get_short_term_event_weights,
    log_recommend_config,
)


# =========================================================
# CẤU HÌNH
# =========================================================
SHORT_TERM_EVENT_WEIGHTS = DEFAULT_RECOMMEND_CONFIG["shortTermEventWeights"]

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
def _normalize_topic(topic):
    if not topic or not isinstance(topic, str):
        return None
    topic = topic.strip().lower()
    return topic if topic else None

def _extract_topics_from_log_metadata(metadata):
    """
    Ưu tiên lấy topic trực tiếp từ log tracking FE.
    Hỗ trợ các dạng:
    - topics: ["data science", "machine learning"]
    - topics: [{name: "..."}]
    - topics: [{inTopic: {name: "..."}}]
    """
    raw_topics = metadata.get("topics", [])
    extracted = []

    if not isinstance(raw_topics, list):
        return []

    for t in raw_topics:
        if isinstance(t, str):
            normalized = _normalize_topic(t)
            if normalized:
                extracted.append(normalized)
        elif isinstance(t, dict):
            name = t.get("name") or ((t.get("inTopic") or {}).get("name"))
            normalized = _normalize_topic(name)
            if normalized:
                extracted.append(normalized)

    return list(dict.fromkeys(extracted))

def _parse_time(raw_ts):
    if raw_ts is None:
        return None

    if isinstance(raw_ts, datetime):
        return raw_ts.replace(tzinfo=None)

    if isinstance(raw_ts, str):
        try:
            return datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None

    return None


def _time_decay(log_time, now, window_days=7):
    if log_time is None:
        return 0.7

    age_days = max(0.0, (now - log_time).total_seconds() / 86400.0)
    decay = 1.0 - (age_days / window_days)
    return max(0.3, min(1.0, decay))


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


# =========================================================
# KỊCH BẢN 1: ONBOARDING TOPICS
# =========================================================
def get_user_onboarding_topics(user_id):
    """[KỊCH BẢN 1] Lấy sở thích dài hạn từ PostgreSQL (Onboarding)"""
    conn = get_postgres_connection()
    cur = conn.cursor()
    try:
        query = """
            SELECT t.name
            FROM "TopicUserInteresteds" tui
            JOIN "Topics" t ON tui."topicId" = t.id
            WHERE tui."userId" = %s
        """
        cur.execute(query, (user_id,))
        results = [_normalize_topic(row[0]) for row in cur.fetchall() if row and row[0]]
        results = [x for x in results if x]
        print(f"   [DEBUG-TOPIC] User {user_id} có các topic onboarding: {results}")
        return results
    except Exception as e:
        print(f"Lỗi lấy Onboarding Topics (User {user_id}): {e}")
        return []
    finally:
        cur.close()
        conn.close()


# =========================================================
# LẤY TOPIC TỪ CONFERENCE IDs
# =========================================================
def get_topics_from_conference_ids(conference_ids):
    """
    Đọc topic từ collection conferences.
    Hỗ trợ cả 2 schema phổ biến:
    1) topics: [str, str, ...]
    2) organizations[].topics[].inTopic.name
    """
    if not conference_ids:
        return {}

    db = get_mongodb_connection()
    collection = db["conferences"]

    conf_ids = list({str(x) for x in conference_ids if x})

    # Thử theo field id trước vì log conference_id thường là UUID string
    confs = list(collection.find({"id": {"$in": conf_ids}}))

    # Nếu không có, thử theo _id string
    if not confs:
        confs = list(collection.find({"_id": {"$in": conf_ids}}))

    conf_topic_map = {}

    for conf in confs:
        conf_id = str(conf.get("id") or conf.get("_id"))
        topics = []

        # Schema 1: topics là mảng string
        raw_topics = conf.get("topics", [])
        if isinstance(raw_topics, list):
            for t in raw_topics:
                normalized = _normalize_topic(t)
                if normalized:
                    topics.append(normalized)

        # Schema 2: organizations[].topics[].inTopic.name
        for org in conf.get("organizations", []):
            for topic in org.get("topics", []):
                name = topic.get("inTopic", {}).get("name")
                normalized = _normalize_topic(name)
                if normalized:
                    topics.append(normalized)

        conf_topic_map[conf_id] = list(dict.fromkeys(topics))

    return conf_topic_map


# =========================================================
# KỊCH BẢN 3: SHORT-TERM TOPICS
# =========================================================
def get_conference_labels(pg_cur, conference_ids):
    if not conference_ids:
        return {}

    ordered_ids = [str(cid) for cid in conference_ids if cid]
    unique_ids = list(dict.fromkeys(ordered_ids))

    pg_cur.execute(
        'SELECT id, acronym, title FROM "Conferences" WHERE id = ANY(%s)',
        (unique_ids,)
    )

    return {
        str(row[0]): {
            "acronym": row[1] or "",
            "title": row[2] or "",
        }
        for row in pg_cur.fetchall()
    }


def log_final_redis_recommendations(user_id, final_recs_with_scores, pg_cur):
    final_ids = [str(item.get("id")) for item in final_recs_with_scores if item.get("id")]
    label_map = get_conference_labels(pg_cur, final_ids)

    print(f"\n   ===== FINAL REDIS RECOMMENDATIONS user={user_id} =====")
    for idx, item in enumerate(final_recs_with_scores, start=1):
        cid = str(item.get("id"))
        score = float(item.get("score", 0))
        label = label_map.get(cid, {})
        acronym = label.get("acronym") or "N/A"
        print(f"   {idx:02d}. {acronym} | score={score:.4f}")

    print("   ===============================================\n")


def get_recent_behavior_topics(user_id, days=7, top_k=3, return_scores=False):
    """[KỊCH BẢN 3] Xác định topic quan tâm từ hành vi gần đây (Short-term)"""
    try:
        db = get_mongodb_connection()
        collection = db["raw_user_logs"]
        now = datetime.utcnow()
        config = get_recommend_config()
        short_term_event_weights = get_short_term_event_weights(config)
        recent_tracking_limit = int(config.get("recentTrackingLimit", 60))
        print(
            "[CONFIG] Recent behavior topics -> "
            f"user={user_id}, trackingLimit={recent_tracking_limit}, "
            f"days={days}, topK={top_k}, events={list(short_term_event_weights.keys())}"
        )

        # [MỚI] Bỏ lọc theo ngày. Lấy tối đa 14 trackings mới nhất 
        # (Tương đương 7 sessions x 2 trackings/session)
        cursor = collection.find({
            "user_id": user_id,
            "event_type": {"$in": list(short_term_event_weights.keys())}
        }).sort("timestamp", -1).limit(recent_tracking_limit)
        
        logs = list(cursor)

        if not logs:
            return []

        conference_ids = []
        for log in logs:
            metadata = log.get("metadata", {}) or {}
            conf_id = metadata.get("conference_id")
            topics_from_log = _extract_topics_from_log_metadata(metadata)

            # Chỉ cần fallback sang Mongo conferences nếu log không có topics
            if conf_id and not topics_from_log:
                conference_ids.append(str(conf_id))

        conf_topic_map = get_topics_from_conference_ids(conference_ids)
        topic_scores = defaultdict(float)

        for log in logs:
            event_type = log.get("event_type")
            metadata = log.get("metadata", {}) or {}
            action = (metadata.get("action") or "").upper()
            conf_id = metadata.get("conference_id")

            base_weight = short_term_event_weights.get(event_type, 0.0)

            if event_type == "SUBMIT_FEEDBACK":
                base_weight = _score_feedback(metadata, base_weight)
            elif event_type == "TIME_ON_PAGE":
                base_weight = _score_time_on_page(metadata, base_weight)
            elif event_type == "SCROLL_DEPTH":
                base_weight = _score_scroll_depth(metadata, base_weight)
            elif (event_type, action) in NEGATIVE_ACTIONS:
                base_weight = -abs(base_weight)
            elif (event_type, action) in POSITIVE_ACTIONS:
                base_weight = abs(base_weight)

            if base_weight == 0 or not conf_id:
                continue

            topics_from_log = _extract_topics_from_log_metadata(metadata)

            if topics_from_log:
                topics = topics_from_log
            else:
                topics = conf_topic_map.get(str(conf_id), [])

            if not topics:
                continue

            log_time = _parse_time(log.get("timestamp"))
            decay = _time_decay(log_time, now, window_days=days)

            for topic in topics:
                topic_scores[topic] += base_weight * decay

        ranked_topics = sorted(topic_scores.items(), key=lambda x: x[1], reverse=True)
        top_topics_with_scores = [
            (topic, score) for topic, score in ranked_topics if score > 0
        ][:top_k]
        top_topics = [topic for topic, _ in top_topics_with_scores]

        print(f"   [DEBUG-SHORT-TERM] User {user_id} topic scores: {ranked_topics[:10]}")
        if return_scores:
            return top_topics_with_scores
        return top_topics   # list[str]
    except Exception as e:
        print(f"Lỗi lấy Short-term Topics (User {user_id}): {e}")
        return []


# =========================================================
# NEGATIVE ITEMS
# =========================================================
def get_negative_items(user_id, days=180):
    """
    Lấy tập các conf user đã reject rõ ràng:
    - UNFOLLOW
    - REMOVE CALENDAR
    """
    try:
        db = get_mongodb_connection()
        collection = db["raw_user_logs"]
        from_time = (datetime.utcnow() - timedelta(days=days)).isoformat()

        logs = collection.find({
            "user_id": user_id,
            "timestamp": {"$gte": from_time},
            "event_type": {"$in": ["CLICK_FOLLOW_STAR", "CLICK_ADD_CALENDAR"]}
        })

        negative_items = set()

        for log in logs:
            metadata = log.get("metadata", {}) or {}
            action = (metadata.get("action") or "").upper()
            event_type = log.get("event_type")
            conf_id = metadata.get("conference_id")

            if conf_id and (event_type, action) in NEGATIVE_ACTIONS:
                negative_items.add(str(conf_id))
            
        return set(map(str, negative_items))
    except Exception as e:
        print(f"Lỗi lấy negative items (User {user_id}): {e}")
        return set()


# =========================================================
# RERANK USER-BASED CF
# =========================================================
def rerank_candidates_for_user(user_id, candidates, user_item_matrix, user_sim_matrix, negative_items=None, top_k_neighbors=10):
    # Chuẩn hóa kiểu dữ liệu để tránh mismatch giữa str / UUID / object
    user_id = str(user_id)
    user_item_matrix.index = user_item_matrix.index.astype(str)
    user_item_matrix.columns = user_item_matrix.columns.astype(str)
    user_sim_matrix.index = user_sim_matrix.index.astype(str)
    user_sim_matrix.columns = user_sim_matrix.columns.astype(str)
    if negative_items is None:
        negative_items = set()

    if user_id not in user_item_matrix.index or user_id not in user_sim_matrix.index:
        return {}

    interacted_items = set(
        map(str, user_item_matrix.columns[user_item_matrix.loc[user_id] > 0])
    )
    negative_items = set(map(str, negative_items))
    print(f"[DEBUG-CF] interacted_items: {interacted_items}")
    print(f"[DEBUG-CF] negative_items: {negative_items}")

    similar_users = user_sim_matrix[user_id].sort_values(ascending=False).index[1: top_k_neighbors + 1]
    recommendation_scores = {}

    # 🔥 DEBUG QUAN TRỌNG
    print(f"[DEBUG-CF] Candidates từ ES: {len(candidates)}")
    print(f"[DEBUG-CF] Matrix columns: {len(user_item_matrix.columns)}")
    print(f"[DEBUG-CF] Candidates nằm trong matrix:",
        set(map(str, candidates)) & set(map(str, user_item_matrix.columns)))

    for cand_id in candidates:
            cand_id = str(cand_id)

            # if negative_items and cand_id in negative_items:
            #     continue

            score_predict = 0.0

            if cand_id in user_item_matrix.columns:
                for sim_user in similar_users:
                    sim_score = user_sim_matrix.loc[user_id, sim_user]
                    rating_of_sim_user = user_item_matrix.loc[sim_user, cand_id] # Hết bị KeyError
                    
                    if sim_score > 0 and rating_of_sim_user > 0:
                        score_predict += sim_score * rating_of_sim_user

            # [QUAN TRỌNG]: Phân bổ điểm
            if score_predict > 0:
                recommendation_scores[cand_id] = score_predict

    return recommendation_scores


# =========================================================
# MAIN
# =========================================================
def run_personalized_recommendation(user_item_matrix, user_sim_matrix):
    """Hàm chính xử lý Kịch bản 1, 3, 4 cho Người 1"""
    print("⏳ [STAGE 2] Đang thực hiện Reranking cá nhân hóa cho từng User...")
    from algorithms.search_and_recommend import get_unified_behavior_topics

    pg_conn = get_postgres_connection()
    pg_cur = pg_conn.cursor()
    redis_client = get_redis_connection()

    # =========================================================
    # CONFIG ADMIN
    # Giữ config mới, nhưng thuật toán bên dưới phục hồi theo logic cũ
    # =========================================================
    config = get_recommend_config()
    log_recommend_config(config, "Personalized Recommendation Job")

    behavior_topic_days = int(config.get("behaviorTopicDays", 7))
    behavior_top_k = int(config.get("behaviorTopK", 3))
    top_k_neighbors = int(config.get("topKNeighbors", 10))

    active_candidate_limit = int(config.get("activeCandidateLimit", 1000))
    final_recommendation_limit = int(config.get("finalRecommendationLimit", 50))
    new_user_candidate_limit = int(config.get("newUserCandidateLimit", 50))
    trending_candidate_limit = int(config.get("trendingCandidateLimit", 50))

    cf_score_weight = float(config.get("cfScoreWeight", 2.5))
    interacted_penalty = float(config.get("interactedPenalty", 5.0))
    negative_item_penalty = float(config.get("negativeItemPenalty", 0.5))

    try:
        # Lấy danh sách tất cả user để tính sẵn kết quả offline
        pg_cur.execute('SELECT id FROM "Users"')
        users = pg_cur.fetchall()

        for u in users:
            user_id = str(u[0])

            # 1. Thu thập topic
            long_term = get_user_onboarding_topics(user_id)

            # =========================================================
            # SHORT-TERM TOPIC
            # Hiện tại vẫn giống code cũ: lấy unified behavior topics.
            #
            # Nếu muốn lọc theo ngày ở hàm get_recent_behavior_topics:
            # from_time = (datetime.utcnow() - timedelta(days=behavior_topic_days)).isoformat()
            # cursor = collection.find({
            #     "user_id": user_id,
            #     "timestamp": {"$gte": from_time},
            #     "event_type": {"$in": list(short_term_event_weights.keys())}
            # }).sort("timestamp", -1).limit(recent_tracking_limit)
            #
            # Còn logic đang dùng hiện tại là bỏ lọc ngày, lấy N tracking mới nhất.
            # =========================================================
            short_term = get_unified_behavior_topics(
                user_id,
                days=behavior_topic_days,
                top_k=behavior_top_k
            )

            print("SHORT TERM:", short_term)
            negative_items = get_negative_items(user_id)

            final_recs = []
            candidates = []
            scored = []
            global_trending_view_map = {}

            # =========================
            # KỊCH BẢN 3: USER ACTIVE
            # =========================
            if short_term:
                print(f"   KỊCH BẢN 3: dùng short-term topics")

                es_results = get_upcoming_candidates_by_topics(
                    short_term,
                    limit=active_candidate_limit
                )

                # ES hiện tại trả object {id, view_count, ...}, nên convert về ID phẳng như code cũ
                candidates = [
                    str(item.get("id") if isinstance(item, dict) else item)
                    for item in es_results
                    if item
                ]

                print(f"🔥 SAU ES candidates: {len(candidates)}")

                if user_id in user_item_matrix.index:
                    recommendation_scores = rerank_candidates_for_user(
                        user_id=user_id,
                        candidates=candidates,
                        user_item_matrix=user_item_matrix,
                        user_sim_matrix=user_sim_matrix,
                        negative_items=negative_items,
                        top_k_neighbors=top_k_neighbors
                    )

                    if recommendation_scores:
                        # =========================
                        # HYBRID RANKING (CF + ES)
                        # Phục hồi logic cũ, chỉ thay số bằng config
                        # =========================
                        scored = []

                        cf_scores = recommendation_scores if recommendation_scores else {}
                        interacted_items = set(
                            map(str, user_item_matrix.columns[user_item_matrix.loc[user_id] > 0])
                        )

                        for idx, cid in enumerate(candidates):
                            cid = str(cid)
                            score = 0.0

                            # ===== FEATURE =====
                            is_interacted = cid in interacted_items
                            cf_score = cf_scores.get(cid, 0.0)

                            # ===== 1. CF quan trọng nhất =====
                            if cf_score > 0:
                                score += cf_score * cf_score_weight

                            # ===== 2. ES ranking =====
                            if len(candidates) > 0:
                                score += (len(candidates) - idx) / len(candidates)

                            # ===== 3. negative =====
                            if cid in negative_items:
                                score -= negative_item_penalty

                            # ===== 4. BUSINESS RULES =====
                            if is_interacted:
                                score -= interacted_penalty

                            scored.append((cid, score))

                        scored.sort(key=lambda x: x[1], reverse=True)

                        print("\n[DEBUG-RANKING]")
                        print("Top 10 scored:")
                        for item in scored[:10]:
                            print(item)

                        final_recs = [cid for cid, _ in scored[:final_recommendation_limit]]

                # fallback của kịch bản 3:
                # nếu CF fail thì dùng heuristic theo topic
                if not final_recs:
                    print(f"   ⚠️ KB3: CF không predict được, fallback heuristic theo topic")
                    scored = []
                    print("DEBUG BEFORE FALLBACK:", len(candidates))
                    interacted_items = set()
                    if user_id in user_item_matrix.index:
                        interacted_items = set(
                            map(str, user_item_matrix.columns[user_item_matrix.loc[user_id] > 0])
                        )

                    for idx, cid in enumerate(candidates):
                        cid = str(cid)
                        score = 0.0

                        # 🔥 thêm rank từ ES - top thì cao -> càng xuống điểm càng thấp
                        if len(candidates) > 0:
                            score += (len(candidates) - idx) / len(candidates)

                        if cid in negative_items:
                            score -= negative_item_penalty

                        if cid in interacted_items:
                            score -= interacted_penalty

                        scored.append((cid, score))

                    scored.sort(key=lambda x: x[1], reverse=True)
                    final_recs = [cid for cid, _ in scored[:final_recommendation_limit]]

                    # nếu heuristic vẫn rỗng thì fallback sang trending toàn cục
                    if not final_recs:
                        print(f"   ⚠️ KB3: heuristic rỗng, fallback trending toàn cục")
                        es_results = get_upcoming_candidates_by_topics(
                            [],
                            limit=trending_candidate_limit
                        )

                        global_trending_view_map = {
                            str(item.get("id")): item.get("view_count", 0)
                            for item in es_results
                            if isinstance(item, dict) and item.get("id")
                        }

                        candidates = [
                            str(item.get("id") if isinstance(item, dict) else item)
                            for item in es_results
                            if item
                        ]

                        final_recs = [
                            cid for cid in candidates
                            if str(cid) not in negative_items
                        ][:final_recommendation_limit]

            # =========================
            # KỊCH BẢN 1: NEW USER
            # =========================
            elif long_term:
                print(f"   KỊCH BẢN 1: NEW USER dùng onboarding topics")

                es_results = get_upcoming_candidates_by_topics(
                    long_term,
                    limit=new_user_candidate_limit
                )

                candidates = [
                    str(item.get("id") if isinstance(item, dict) else item)
                    for item in es_results
                    if item
                ]

                # KHÔNG rerank CF
                final_recs = [
                    cid for cid in candidates
                    if str(cid) not in negative_items
                ][:final_recommendation_limit]

                # nếu vì lý do nào đó topic-based rỗng thì fallback global trending
                if not final_recs:
                    print(f"   ⚠️ KB1: không có candidate theo onboarding topic, fallback trending")
                    es_results = get_upcoming_candidates_by_topics(
                        [],
                        limit=trending_candidate_limit
                    )

                    global_trending_view_map = {
                        str(item.get("id")): item.get("view_count", 0)
                        for item in es_results
                        if isinstance(item, dict) and item.get("id")
                    }

                    candidates = [
                        str(item.get("id") if isinstance(item, dict) else item)
                        for item in es_results
                        if item
                    ]

                    final_recs = [
                        cid for cid in candidates
                        if str(cid) not in negative_items
                    ][:final_recommendation_limit]

            # =========================
            # KỊCH BẢN 4: NO DATA / COLD
            # =========================
            else:
                print(f"   KỊCH BẢN 4: fallback trending")

                es_results = get_upcoming_candidates_by_topics(
                    [],
                    limit=trending_candidate_limit
                )

                global_trending_view_map = {
                    str(item.get("id")): item.get("view_count", 0)
                    for item in es_results
                    if isinstance(item, dict) and item.get("id")
                }

                candidates = [
                    str(item.get("id") if isinstance(item, dict) else item)
                    for item in es_results
                    if item
                ]

                final_recs = [
                    cid for cid in candidates
                    if str(cid) not in negative_items
                ][:final_recommendation_limit]

            print(f"\n🚨 DEBUG FINAL RECS USER {user_id}")
            print(f"   Candidates: {len(candidates)}")
            print(f"   Final recs: {len(final_recs)}")

            # =========================================================
            # ĐÓNG GÓI DỮ LIỆU KÈM SCORE CHO REDIS
            # Giữ format mới để BE đọc được score.
            # =========================================================
            final_recs_with_scores = []

            # Kịch bản 3 có scored thì lưu score thuật toán
            if scored:
                final_recs_with_scores = [
                    {"id": str(cid), "score": float(score)}
                    for cid, score in scored[:final_recommendation_limit]
                ]

            # Kịch bản trending thì ưu tiên score view_count nếu có
            elif global_trending_view_map:
                final_recs_with_scores = [
                    {
                        "id": str(cid),
                        "score": float(global_trending_view_map.get(str(cid), 0))
                    }
                    for cid in final_recs
                ]

            # Kịch bản 1 hoặc fallback thường thì score theo thứ tự ES
            else:
                total_cands = len(final_recs) if final_recs else 1
                final_recs_with_scores = [
                    {
                        "id": str(cid),
                        "score": round(1.0 + (total_cands - idx) / total_cands, 4)
                    }
                    for idx, cid in enumerate(final_recs)
                ]

            # 5. LƯU REDIS
            redis_key = f"recommend_user:{user_id}"
            if final_recs_with_scores:
                log_final_redis_recommendations(user_id, final_recs_with_scores, pg_cur)
                redis_client.set(redis_key, json.dumps(final_recs_with_scores))
                print(f"   ✅ Đã cập nhật Redis (ID + Score) cho User {user_id}")
            else:
                redis_client.delete(redis_key)
                print(f"   ℹ️ Không có kết quả cho User {user_id}, đã xóa Redis key")

    finally:
        pg_cur.close()
        pg_conn.close()
