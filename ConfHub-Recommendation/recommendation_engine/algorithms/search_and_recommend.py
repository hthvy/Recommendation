# File: algorithms/search_and_recommend.py
from datetime import datetime
from core.db_client import get_redis_connection
from core.es_client import get_upcoming_candidates_by_topics
from core.db_client import get_mongodb_connection
from core.recommend_config import get_recommend_config, log_recommend_config
from collections import defaultdict
from datetime import timedelta
from math import sqrt

SEARCHED_TOPIC_WEIGHT_MULTIPLIER = 2.5
SEARCHED_TOPIC_MAX_PER_EVENT = 4.0

def _normalize_topic(topic):
    if not topic or not isinstance(topic, str):
        return None
    
    topic = topic.strip().lower()

    # ❌ loại bỏ topic rác (1-2 ký tự)
    if len(topic) < 3:
        return None

    return topic
# =========================================================
# 1 & 2: AI NGỮ NGHĨA - SỬA LỖI CHÍNH TẢ & TRÍCH XUẤT TOPIC
# =========================================================
def extract_topics_from_keyword(detected_keyword):
    """
    Sử dụng Elasticsearch để tìm kiếm mờ (Fuzzy Search - sửa lỗi chính tả)
    và trích xuất ra Topic gần giống nhất với từ khóa người dùng gõ.
    """
    from core.es_client import es, INDEX_NAME
    
    # Nếu keyword quá ngắn, bỏ qua
    if not detected_keyword or len(detected_keyword.strip()) < 2:
        return {}

    # Query ES: Tìm trong field "topics", cho phép sai tối đa 2 lỗi chính tả (fuzziness: AUTO)
    query = {
        "size": 5, # Lấy 5 hội nghị khớp nhất
        "query": {
            "match": {
                "topics": {
                    "query": detected_keyword,
                    "fuzziness": "AUTO", # Phép màu sửa lỗi chính tả nằm ở đây!
                    "operator": "or"
                }
            }
        }
    }
    
    topic_scores = {}
    try:
        response = es.search(index=INDEX_NAME, body=query)
        hits = response['hits']['hits']
        
        # Bóc tách Topic từ các hội nghị tìm được
        for hit in hits:
            score = float(hit['_score'])
            conf_topics = hit['_source'].get('topics', [])
            
            # Gán điểm ES Score cho từng Topic tìm được
            if isinstance(conf_topics, list):
                for t in conf_topics:
                    norm = _normalize_topic(t)
                    if norm:
                        topic_scores[norm] = topic_scores.get(norm, 0) + score

            elif isinstance(conf_topics, str):
                norm = _normalize_topic(conf_topics)
                if norm:
                    topic_scores[norm] = topic_scores.get(norm, 0) + score
                
    except Exception as e:
        print(f"[AI NLP ERROR] Lỗi khi đoán ngữ nghĩa Topic từ ES: {e}")
        
    # =======================================================
    # BƯỚC MỚI: SẮP XẾP VÀ CHỈ LẤY TOP 3 TOPIC ĐIỂM CAO NHẤT
    # =======================================================
    # 1. In ra toàn bộ Topics (rác + chuẩn) trước khi cắt
    # print(f"      🔍 [DEBUG NLP] Tất cả Topic bóc được từ ES: {topic_scores}")
    
    # 2. Sắp xếp dictionary theo điểm (từ cao xuống thấp)
    sorted_topics = sorted(topic_scores.items(), key=lambda item: item[1], reverse=True)
    
    # 3. Cắt lấy đúng 3 cái đầu tiên và chuyển lại thành Dictionary
    top_3_topics = dict(sorted_topics[:3])
    
    # 4. In ra kết quả Top 3 tinh túy nhất sau khi cắt
    # print(f"      ✂️ [DEBUG NLP] Đã lọc nhiễu, chỉ giữ lại Top 3 Topic Core: {top_3_topics}")
    # print("RAW TOPICS FROM ES:", conf_topics)
    return top_3_topics

def process_search_query(raw_keyword):
    """Giữ nguyên chữ thường để ném xuống ES xử lý"""
    return str(raw_keyword).lower().strip() if raw_keyword else ""

def extract_searched_topics_from_filters(metadata):
    filters = metadata.get("filters", {})
    if not isinstance(filters, dict):
        return []

    raw_topics = filters.get("topics", [])
    if not isinstance(raw_topics, list):
        raw_topics = [raw_topics]

    searched_topics = []
    for raw_topic in raw_topics:
        if isinstance(raw_topic, str):
            topic_query = raw_topic
        elif isinstance(raw_topic, dict):
            topic_query = (
                raw_topic.get("name")
                or raw_topic.get("label")
                or raw_topic.get("value")
                or ((raw_topic.get("inTopic") or {}).get("name"))
            )
        else:
            topic_query = None

        normalized = process_search_query(topic_query)
        if normalized and len(normalized) >= 2 and normalized not in searched_topics:
            searched_topics.append(normalized)

    return searched_topics

def add_capped_topic_scores(target_scores, topic_scores, max_total):
    positive_total = sum(score for score in topic_scores.values() if score > 0)
    if positive_total <= 0:
        return

    scale = min(1.0, max_total / positive_total)
    for topic, score in topic_scores.items():
        if score > 0:
            target_scores[topic] += score * scale
# =========================================================
# XỬ LÝ LOG SEARCH (WRAPPER)
# =========================================================
def get_search_topic_scores(user_id, days=7):
    """Quét MongoDB lấy lịch sử gõ Keyword và quy ra điểm Topic"""
    try:
        from core.db_client import get_mongodb_connection
        db = get_mongodb_connection()
        collection = db["raw_user_logs"]
        config = get_recommend_config()
        search_intent_weight = float(config.get("searchIntentWeight", 0.35))
        print(
            "[CONFIG] Search topic scores -> "
            f"user={user_id}, days={days}, searchIntentWeight={search_intent_weight}"
        )
        now = datetime.utcnow()
        from_time = (now - timedelta(days=days)).isoformat()

        # NỚI LỎNG ĐIỀU KIỆN: Hỗ trợ cả 2 tên event mà hệ thống có thể lưu
        logs = list(collection.find({
            "user_id": user_id,
            "timestamp": {"$gte": from_time},
            "event_type": {"$in": ["SEARCH_CONFERENCE", "search", "SEARCH"]} 
        }))

        search_scores = defaultdict(float)
        for log in logs:
            # Dữ liệu Frontend truyền xuống có thể nằm ở cột 'metadata' hoặc 'content'
            metadata = log.get("metadata") or log.get("content") or {}
            
            # GIẢI MÃ CHUỖI JSON TỪ FRONTEND:
            if isinstance(metadata, str):
                import json
                try: 
                    metadata = json.loads(metadata)
                except: 
                    metadata = {}
            
            keyword = metadata.get("keyword")
            if keyword:
                print(f"\n   🤖 [AI NGỮ NGHĨA] Đang phân tích hành vi Search cũ của User {user_id[:5]}...")
                print(f"      > Phát hiện User từng gõ chữ: '{keyword}'")
                
                detected_kw = process_search_query(keyword)
                topics_with_score = extract_topics_from_keyword(detected_kw)
                
                print(f"      > AI đã dịch ngữ nghĩa thành Topics: {topics_with_score}")
                for topic, score in topics_with_score.items():
                    search_scores[topic] += (score * search_intent_weight)

            searched_topic_queries = extract_searched_topics_from_filters(metadata)
            if searched_topic_queries:
                per_topic_multiplier = SEARCHED_TOPIC_WEIGHT_MULTIPLIER / sqrt(len(searched_topic_queries))
                searched_topic_scores = defaultdict(float)

                print(f"\n   [SEARCH TOPIC BOOST] Phat hien topic user search/filter: {searched_topic_queries}")
                for searched_topic_query in searched_topic_queries:
                    topics_with_score = extract_topics_from_keyword(searched_topic_query)
                    print(f"      > Topic query '{searched_topic_query}' resolve thanh: {topics_with_score}")

                    for topic, score in topics_with_score.items():
                        searched_topic_scores[topic] += (
                            score * search_intent_weight * per_topic_multiplier
                        )

                add_capped_topic_scores(
                    search_scores,
                    searched_topic_scores,
                    SEARCHED_TOPIC_MAX_PER_EVENT,
                )
                    
        return search_scores
    except Exception as e:
        print(f"Lỗi đọc log search: {e}")
        return {}

def get_unified_behavior_topics(user_id, days=7, top_k=3):

    from algorithms.user_based_cf import get_recent_behavior_topics
    config = get_recommend_config()
    long_term_topic_limit = int(config.get("longTermTopicLimit", 10))
    search_history_days = int(config.get("searchHistoryDays", 2))
    search_boost_weight = float(config.get("searchBoostWeight", 0.25))
    print(
        "[CONFIG] Unified behavior topics -> "
        f"user={user_id}, behaviorDays={days}, finalTopK={top_k}, "
        f"longTermTopicLimit={long_term_topic_limit}, "
        f"searchHistoryDays={search_history_days}, searchBoostWeight={search_boost_weight}"
    )

    # =====================================================
    # 1. LONG-TERM CORE PREFERENCE
    # =====================================================
    old_topics = get_recent_behavior_topics(
        user_id,
        days=days,
        top_k=long_term_topic_limit,
        return_scores=True
    )

    # =====================================================
    # 2. SHORT-TERM SEARCH INTENT
    # =====================================================
    search_scores = get_search_topic_scores(
        user_id,
        days=search_history_days
    )

    final_scores = defaultdict(float)

    print(f"\n{'='*65}")
    print("🧠 [USER TOPIC FUSION PIPELINE]")
    print(f"{'='*65}")

    # =====================================================
    # 3. CORE PREFERENCE SCORING
    # =====================================================
    print(f"\n🧠 [LONG-TERM CORE TOPICS]")

    for topic, core_score in old_topics:

        # Dung diem behavior that da tinh trong get_recent_behavior_topics.
        final_scores[topic] += core_score

        print(f"   + {topic:<25} : {core_score:.2f}")

    # =====================================================
    # 4. SEARCH BOOST
    # =====================================================
    print(f"\n🔥 [SHORT-TERM SEARCH BOOST]")

    if not search_scores:
        print("   -> Không có search history")

    for topic, score in search_scores.items():

        # Search chỉ boost nhẹ
        boost_score = score * search_boost_weight

        final_scores[topic] += boost_score

        print(
            f"   + {topic:<25} : "
            f"RAW_ES={score:.2f} "
            f"=> BOOST={boost_score:.2f}"
        )

    # =====================================================
    # 5. FINAL MERGED SCORES
    # =====================================================
    ranked = sorted(
        final_scores.items(),
        key=lambda x: x[1],
        reverse=True
    )

    print(f"\n🏆 [FINAL TOPIC SCORES]")

    print(f"   {'TOPIC':<30} | {'TOTAL SCORE':<12}")
    print(f"   {'-'*48}")

    for topic, score in ranked:
        print(f"   {topic:<30} | {score:.2f}")

    # =====================================================
    # 6. FINAL TOP K
    # =====================================================
    final_topics = [
        t for t, s in ranked[:top_k]
        if t and len(t) >= 3
    ]

    print(f"\n🎯 [FINAL SELECTED TOP {top_k} TOPICS]")

    for idx, (topic, score) in enumerate(ranked[:top_k], start=1):
        print(
            f"   {idx}. "
            f"{topic:<25} "
            f"(score={score:.2f})"
        )

    print(f"\n{'='*65}")

    return final_topics
# =========================================================
# 3. LUỒNG CHÍNH: TÌM KIẾM & TƯ VẤN CÁ NHÂN HÓA
# =========================================================
def run_smart_search_and_recommend(user_id, raw_keyword, user_item_matrix, user_sim_matrix):
    config = get_recommend_config()
    log_recommend_config(config, "Smart Search Recommendation")
    smart_search_candidate_limit = int(config.get("smartSearchCandidateLimit", 50))
    smart_search_cf_weight = float(config.get("smartSearchCfWeight", 2.0))
    top_k_neighbors = int(config.get("topKNeighbors", 10))

    print(f"\n{'='*70}")
    print(f"🔎 [SMART SEARCH PIPELINE] BẮT ĐẦU XỬ LÝ")
    print(f"{'-'*70}")

    # ---------------------------------------------------------
    # BƯỚC 1: XỬ LÝ TỪ KHÓA (RAW -> DETECTED)
    # ---------------------------------------------------------
    detected_keyword = process_search_query(raw_keyword)
    print(f"📝 [LOG 1 - TEXT DETECT]")
    print(f"   -> User gõ (Raw)      : '{raw_keyword}'")
    print(f"   -> AI Nhận diện (Chuẩn): '{detected_keyword}'")

    # ---------------------------------------------------------
    # BƯỚC 2: TÍNH TOÁN & TRÍCH XUẤT TOP 3 TOPICS TỪ KEYWORD
    # ---------------------------------------------------------
    topic_scores = extract_topics_from_keyword(detected_keyword)
    print(f"\n🧠 [LOG 2 - KEYWORD TO TOPICS]")
    print(f"   -> AI đã tìm thấy {len(topic_scores)} Topics liên quan đến từ khóa.")
    
    # Sắp xếp topic theo điểm giảm dần
    sorted_topics = sorted(topic_scores.items(), key=lambda x: x[1], reverse=True)
    
    print(f"   -> Bảng điểm Topic chi tiết:")
    for topic, score in sorted_topics:
        print(f"      + {topic:<20} : {score:.2f} điểm")
        
    # CHỈ LẤY TOP 3 TOPIC CAO ĐIỂM NHẤT NHƯ YÊU CẦU
    top_3_search_topics = [t[0] for t in sorted_topics[:3]]
    print(f"   🎯 => CHỐT LẤY TOP 3 TOPICS ĐỂ TÌM KIẾM: {top_3_search_topics}")
    print(f"\n🔥 [SEARCH HOT TOPICS]")
    for idx, topic in enumerate(top_3_search_topics, start=1):
        print(f"   {idx}. {topic}")
    # ---------------------------------------------------------
    # BƯỚC 3: KẾT HỢP NGỮ CẢNH CÁ NHÂN (OPTIONAL)
    # ---------------------------------------------------------
    # Để kết quả search mang đậm tính cá nhân, ta có thể trộn Top 3 Topic của Keyword 
    # với Top Topic mà User này hay xem trong quá khứ.
    final_target_topics = top_3_search_topics
    
    print(f"\n👤 [LOG 3 - USER CONTEXT]")
    if history_topics:
        print(f"   -> Lịch sử User thích các Topics: {history_topics}")
        print(f"   -> Tập Topic Hợp Nhất (Search + Lịch sử): {final_target_topics}")
    else:
        print(f"   -> User chưa có lịch sử. Dùng 100% Topic từ Search: {final_target_topics}")

    # ---------------------------------------------------------
    # BƯỚC 4: TÌM KIẾM ỨNG VIÊN BẰNG ELASTICSEARCH
    # ---------------------------------------------------------
    print(f"\n📦 [LOG 4 - ELASTICSEARCH RETRIEVAL]")
    # Tái sử dụng hàm đã có của đồ án: Tìm hội nghị dựa trên mảng Topics
    candidates = get_upcoming_candidates_by_topics(final_target_topics, limit=smart_search_candidate_limit)
    
    # Lọc rác
    negative_items = get_negative_items(user_id)
    valid_candidates = [str(cid) for cid in candidates if str(cid) not in negative_items]
    print(f"   -> ES tìm được {len(candidates)} hội nghị khớp Topic.")
    print(f"   -> Sau khi lọc rác (Unfollow), còn lại {len(valid_candidates)} ứng viên.")

    # ---------------------------------------------------------
    # BƯỚC 5: CHẤM ĐIỂM CÁ NHÂN HÓA (USER-BASED CF)
    # ---------------------------------------------------------
    print(f"\n⚙️ [LOG 5 - RERANKING BẰNG USER-BASED CF]")
    cf_scores = {}
    if user_id in user_item_matrix.index and user_id in user_sim_matrix.index:
        cf_scores = rerank_candidates_for_user(
            user_id=user_id,
            candidates=valid_candidates,
            user_item_matrix=user_item_matrix,
            user_sim_matrix=user_sim_matrix,
            negative_items=negative_items,
            top_k_neighbors=top_k_neighbors
        )
        print(f"   -> Thuật toán User-based CF đã tính được Rating cho {len(cf_scores)} hội nghị.")
    else:
        print(f"   -> User mới/Chưa có ma trận Rating. Bỏ qua CF Reranking.")

    # ---------------------------------------------------------
    # BƯỚC 6: TỔNG HỢP & RANKING CUỐI CÙNG
    # ---------------------------------------------------------
    final_results = []
    
    for conf_id in valid_candidates:
        # Ở đây ta giả định Base Score (điểm nền từ Topic) là 1.0
        # Điểm cá nhân hóa từ CF (nếu có) sẽ được cộng dồn vào
        cf_score = cf_scores.get(conf_id, 0.0)
        total_score = 1.0 + (cf_score * smart_search_cf_weight) # Nhân đôi trọng số sở thích
        
        final_results.append({
            "conf_id": conf_id,
            "cf_score": cf_score,
            "total_score": total_score
        })

    # Sắp xếp giảm dần theo tổng điểm
    final_results.sort(key=lambda x: x['total_score'], reverse=True)

    print(f"\n🏆 [LOG 6 - KẾT QUẢ TƯ VẤN TÌM KIẾM TỐT NHẤT (TOP 5)]")
    print(f"   {'-'*55}")
    print(f"   {'Conf_ID':<24} | {'CF_Rating':<10} | {'TOTAL_SCORE':<10}")
    print(f"   {'-'*55}")
    for r in final_results[:5]:
        print(f"   {r['conf_id']:<24} | {r['cf_score']:<10.4f} | {r['total_score']:<10.4f}")

    print(f"{'='*70}\n")
    
    # Trả về mảng ID để lưu Redis hoặc ném về API
    return [r['conf_id'] for r in final_results]

def precompute_all_user_cf_scores(user_item_matrix, user_sim_matrix):
    """
    [LUỒNG OFFLINE] Tính trước điểm CF cho tất cả User và lưu lên Redis (Hash Map).
    """
    print("\n⏳ [OFFLINE JOB] Đang tính trước bảng điểm CF cho tất cả User...")
    from core.db_client import get_redis_connection
    redis_client = get_redis_connection()
    config = get_recommend_config()
    log_recommend_config(config, "Offline CF Precompute")
    offline_cf_neighbor_limit = int(config.get("offlineCfNeighborLimit", 10))
    offline_own_rating_weight = float(config.get("offlineOwnRatingWeight", 2.0))
    offline_cf_ttl_seconds = int(config.get("offlineCfTtlSeconds", 86400))
    all_conferences = user_item_matrix.columns.tolist()
    
    for user_id in user_item_matrix.index:
        similar_users = user_sim_matrix[user_id].sort_values(ascending=False).index[1:offline_cf_neighbor_limit + 1]
        user_scores = {}
        
        for conf_id in all_conferences:
            # 1. ĐIỂM CỦA CHÍNH USER ĐÃ TƯƠNG TÁC (Phá băng Data Sparsity)
            my_own_rating = float(user_item_matrix.loc[user_id, conf_id])
            score_predict = my_own_rating * offline_own_rating_weight  # Nhân đôi trọng số để ưu tiên lên Top
            
            # 2. CỘNG THÊM ĐIỂM TỪ HÀNG XÓM (CF)
            for sim_user in similar_users:
                sim_score = user_sim_matrix.loc[user_id, sim_user]
                rating_of_sim_user = user_item_matrix.loc[sim_user, conf_id]
                if sim_score > 0 and rating_of_sim_user > 0:
                    score_predict += sim_score * rating_of_sim_user
                    
            if score_predict > 0:
                user_scores[str(conf_id)] = round(float(score_predict), 4)
                
        # 3. LƯU VÀO REDIS
        redis_key = f"user_cf_scores:{user_id}"
        if user_scores:
            redis_client.delete(redis_key) 
            redis_client.hset(redis_key, mapping=user_scores)
            redis_client.expire(redis_key, offline_cf_ttl_seconds)
            
    print("✅ [OFFLINE JOB] Đã tính xong và nạp bảng điểm Search lên Redis thành công!")
