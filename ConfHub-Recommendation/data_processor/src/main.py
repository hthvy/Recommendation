import json
import os
import time
from datetime import datetime, timezone
from math import sqrt

from kafka import KafkaConsumer
from pymongo import MongoClient
import redis

from filters.manager import FilterManager


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

REALTIME_TRENDING_TTL_SECONDS = int(
    os.getenv("REALTIME_TRENDING_TTL_SECONDS", str(14 * 24 * 60 * 60))
)
REALTIME_TOPIC_TTL_SECONDS = int(
    os.getenv("REALTIME_TOPIC_TTL_SECONDS", str(14 * 24 * 60 * 60))
)
REALTIME_DEDUP_SECONDS = int(os.getenv("REALTIME_DEDUP_SECONDS", "60"))
REALTIME_RECENT_TRACKING_LIMIT = int(
    os.getenv("REALTIME_RECENT_TRACKING_LIMIT", "60")
)

TRENDING_EVENTS = {
    "CLICK_CONFERENCE_TITLE",
    "CLICK_RELATED_DETAIL",
}

SHORT_TOPIC_EVENT_WEIGHTS = {
    "VIEW_CONFERENCE_DETAIL": 1.8,
    "CLICK_CONFERENCE_TITLE": 2.0,
    "CLICK_RELATED_DETAIL": 2.0,
    "CLICK_EXTERNAL_WEBSITE": 2.5,
    "CLICK_FOLLOW_STAR": 3.0,
    "CLICK_ADD_CALENDAR": 2.8,
    "SUBMIT_FEEDBACK": 2.0,
    "TIME_ON_PAGE": 1.2,
    "SCROLL_DEPTH": 1.0,
    "SEARCH_CONFERENCE": 3.5,
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

filter_manager = FilterManager()


def normalize_topic(topic):
    if not topic or not isinstance(topic, str):
        return None
    topic = topic.strip().lower()
    return topic if topic else None


def event_date_key(log_data):
    raw_ts = log_data.get("timestamp")
    if isinstance(raw_ts, str):
        try:
            parsed = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def extract_topics_from_metadata(metadata):
    raw_topics = metadata.get("topics", [])
    extracted = []

    if not isinstance(raw_topics, list):
        return []

    for topic in raw_topics:
        if isinstance(topic, str):
            normalized = normalize_topic(topic)
        elif isinstance(topic, dict):
            normalized = normalize_topic(
                topic.get("name") or ((topic.get("inTopic") or {}).get("name"))
            )
        else:
            normalized = None

        if normalized and normalized not in extracted:
            extracted.append(normalized)

    return extracted


def extract_search_topics_from_filters(metadata):
    filters = metadata.get("filters", {})
    if not isinstance(filters, dict):
        return []

    raw_topics = filters.get("topics", [])
    if not isinstance(raw_topics, list):
        raw_topics = [raw_topics]

    extracted = []
    for topic in raw_topics:
        if isinstance(topic, str):
            normalized = normalize_topic(topic)
        elif isinstance(topic, dict):
            normalized = normalize_topic(
                topic.get("name")
                or topic.get("label")
                or topic.get("value")
                or ((topic.get("inTopic") or {}).get("name"))
            )
        else:
            normalized = None

        if normalized and normalized not in extracted:
            extracted.append(normalized)

    return extracted


def get_topics_from_conference(collection, conf_id):
    if not conf_id:
        return []

    try:
        db = collection.database
        conf = db["conferences"].find_one({"id": str(conf_id)}) or db[
            "conferences"
        ].find_one({"_id": str(conf_id)})
        if not conf:
            return []

        topics = []
        raw_topics = conf.get("topics", [])

        if isinstance(raw_topics, list):
            for topic in raw_topics:
                normalized = normalize_topic(topic)
                if normalized and normalized not in topics:
                    topics.append(normalized)

        for org in conf.get("organizations", []):
            for topic in org.get("topics", []):
                normalized = normalize_topic((topic.get("inTopic") or {}).get("name"))
                if normalized and normalized not in topics:
                    topics.append(normalized)

        return topics
    except Exception as error:
        print(
            f"[REALTIME] Could not load conference topics for {conf_id}: {error}",
            flush=True,
        )
        return []


def compute_short_topic_weight(log_data):
    event_type = log_data.get("event_type")
    metadata = log_data.get("metadata", {}) or {}
    action = str(metadata.get("action") or "").upper()
    weight = float(SHORT_TOPIC_EVENT_WEIGHTS.get(event_type, 0.0))

    if event_type == "SUBMIT_FEEDBACK":
        rating = metadata.get("rating_score") or metadata.get("rating")
        try:
            rating = int(rating)
        except Exception:
            rating = 3

        if rating >= 4:
            return abs(weight)
        if rating == 3:
            return abs(weight) * 0.5
        return -abs(weight)

    if event_type == "TIME_ON_PAGE":
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
            return weight
        if seconds >= 90:
            return weight * 0.75
        if seconds >= 30:
            return weight * (5.0 / 12.0)
        return 0.0

    if event_type == "SCROLL_DEPTH":
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
            return weight
        if percent >= 75:
            return weight * 0.8
        if percent >= 50:
            return weight * 0.5
        if percent >= 25:
            return weight * 0.2
        return 0.0

    if (event_type, action) in NEGATIVE_ACTIONS:
        return -abs(weight)
    if (event_type, action) in POSITIVE_ACTIONS:
        return abs(weight)

    return weight


def is_negative_interaction(log_data):
    event_type = log_data.get("event_type")
    metadata = log_data.get("metadata", {}) or {}
    action = str(metadata.get("action") or "").upper()
    return (event_type, action) in NEGATIVE_ACTIONS


def should_count_event(redis_client, user_id, conf_id, event_type):
    if not user_id or not conf_id or REALTIME_DEDUP_SECONDS <= 0:
        return True

    dedup_key = f"realtime:dedup:{user_id}:{conf_id}:{event_type}"
    try:
        return bool(redis_client.set(dedup_key, "1", nx=True, ex=REALTIME_DEDUP_SECONDS))
    except Exception as error:
        print(f"[REALTIME] Dedup failed, counting event anyway: {error}", flush=True)
        return True


def update_realtime_recommendation_signals(redis_client, collection, log_data):
    event_type = log_data.get("event_type")
    metadata = log_data.get("metadata", {}) or {}
    conf_id = metadata.get("conference_id")
    user_id = log_data.get("user_id")
    guest_id = log_data.get("guest_id") or metadata.get("guest_id")
    guest_session_id = (
        log_data.get("guest_session_id") or metadata.get("guest_session_id")
    )
    actor_id = user_id
    if user_id == "guest" and guest_id and guest_session_id:
        actor_id = f"guest:{guest_id}:{guest_session_id}"
    date_key = event_date_key(log_data)

    if user_id and user_id != "guest" and event_type in {"SEARCH_CONFERENCE", "search", "SEARCH"}:
        keyword = metadata.get("keyword")
        if keyword:
            search_keyword_key = f"user:search_keywords:{user_id}:{date_key}"
            redis_client.zincrby(search_keyword_key, 1, str(keyword).strip().lower())
            redis_client.expire(search_keyword_key, REALTIME_TOPIC_TTL_SECONDS)
        searched_topics = extract_search_topics_from_filters(metadata)
        if searched_topics:
            search_topic_key = f"user:search_topics:{user_id}:{date_key}"
            topic_increment = 1 / sqrt(len(searched_topics))
            for topic in searched_topics:
                redis_client.zincrby(search_topic_key, topic_increment, topic)
            redis_client.expire(search_topic_key, REALTIME_TOPIC_TTL_SECONDS)

    if user_id == "guest" and guest_id and guest_session_id and event_type in {"SEARCH_CONFERENCE", "search", "SEARCH"}:
        keyword = metadata.get("keyword")
        if keyword:
            guest_scope = f"{guest_id}:{guest_session_id}"
            search_keyword_key = f"guest:search_keywords:{guest_scope}:{date_key}"
            redis_client.zincrby(search_keyword_key, 1, str(keyword).strip().lower())
            redis_client.expire(search_keyword_key, REALTIME_TOPIC_TTL_SECONDS)
        searched_topics = extract_search_topics_from_filters(metadata)
        if searched_topics:
            guest_scope = f"{guest_id}:{guest_session_id}"
            search_topic_key = f"guest:search_topics:{guest_scope}:{date_key}"
            topic_increment = 1 / sqrt(len(searched_topics))
            for topic in searched_topics:
                redis_client.zincrby(search_topic_key, topic_increment, topic)
            redis_client.expire(search_topic_key, REALTIME_TOPIC_TTL_SECONDS)

    if not conf_id:
        if user_id == "guest":
            print(
                f"[REALTIME SKIP] guest event={event_type} reason=missing_conference_id "
                f"guest_id={guest_id or 'none'} session={guest_session_id or 'none'}",
                flush=True,
            )
        return

    conf_id = str(conf_id)
    topics = extract_topics_from_metadata(metadata)

    if not topics:
        topics = get_topics_from_conference(collection, conf_id)

    if event_type in TRENDING_EVENTS and should_count_event(
        redis_client, actor_id, conf_id, event_type
    ):
        global_key = f"trending:global:{date_key}"
        redis_client.zincrby(global_key, 1, conf_id)
        redis_client.expire(global_key, REALTIME_TRENDING_TTL_SECONDS)

    if user_id and user_id != "guest":
        topic_weight = compute_short_topic_weight(log_data)
        interacted_key = f"user:recent_interacted:{user_id}:{date_key}"
        negative_key = f"user:negative_items:{user_id}:{date_key}"

        if topic_weight > 0:
            redis_client.sadd(interacted_key, conf_id)
            redis_client.expire(interacted_key, REALTIME_TOPIC_TTL_SECONDS)

        if is_negative_interaction(log_data):
            redis_client.sadd(negative_key, conf_id)
            redis_client.expire(negative_key, REALTIME_TOPIC_TTL_SECONDS)

        if topic_weight != 0 and topics:
            topic_key = f"user:short_topics:{user_id}:{date_key}"
            for topic in topics:
                redis_client.zincrby(topic_key, topic_weight, topic)
            redis_client.expire(topic_key, REALTIME_TOPIC_TTL_SECONDS)

            recent_behavior_key = f"user:recent_behavior_events:{user_id}"
            redis_client.lpush(
                recent_behavior_key,
                json.dumps(
                    {
                        "timestamp": log_data.get("timestamp"),
                        "event_type": event_type,
                        "conf_id": conf_id,
                        "topics": topics,
                        "weight": topic_weight,
                    }
                ),
            )
            redis_client.ltrim(
                recent_behavior_key,
                0,
                max(0, REALTIME_RECENT_TRACKING_LIMIT - 1),
            )
            redis_client.expire(recent_behavior_key, REALTIME_TOPIC_TTL_SECONDS)

            redis_client.set(
                f"user:last_interaction_at:{user_id}",
                datetime.now(timezone.utc).isoformat(),
                ex=REALTIME_TOPIC_TTL_SECONDS,
            )

    if user_id == "guest" and guest_id and guest_session_id:
        topic_weight = compute_short_topic_weight(log_data)
        guest_scope = f"{guest_id}:{guest_session_id}"
        interacted_key = f"guest:recent_interacted:{guest_scope}"

        if topic_weight > 0:
            redis_client.sadd(interacted_key, conf_id)
            redis_client.expire(interacted_key, REALTIME_TOPIC_TTL_SECONDS)

        if topic_weight != 0 and topics:
            recent_behavior_key = f"guest:recent_behavior_events:{guest_scope}"
            redis_client.lpush(
                recent_behavior_key,
                json.dumps(
                    {
                        "timestamp": log_data.get("timestamp"),
                        "event_type": event_type,
                        "conf_id": conf_id,
                        "topics": topics,
                        "weight": topic_weight,
                        "guest_id": guest_id,
                        "guest_session_id": guest_session_id,
                        "position_on_page": metadata.get("position_on_page"),
                        "absolute_rank": metadata.get("absolute_rank"),
                        "clicked_rank": metadata.get("clicked_rank"),
                    }
                ),
            )
            redis_client.expire(recent_behavior_key, REALTIME_TOPIC_TTL_SECONDS)
            redis_client.set(
                f"guest:last_interaction_at:{guest_scope}",
                datetime.now(timezone.utc).isoformat(),
                ex=REALTIME_TOPIC_TTL_SECONDS,
            )
            print(
                f"[REALTIME GUEST SAVED] key={recent_behavior_key} "
                f"event={event_type} conf={conf_id} topics={len(topics)} weight={topic_weight}",
                flush=True,
            )
        else:
            print(
                f"[REALTIME GUEST SKIP] event={event_type} conf={conf_id} "
                f"guest_id={guest_id} session={guest_session_id} "
                f"topics={len(topics)} weight={topic_weight}",
                flush=True,
            )
    elif user_id == "guest":
        print(
            f"[REALTIME GUEST SKIP] event={event_type} conf={conf_id} "
            f"reason=missing_guest_identity guest_id={guest_id or 'none'} "
            f"session={guest_session_id or 'none'}",
            flush=True,
        )


def connect_services():
    while True:
        try:
            print("Connecting to MongoDB, Redis & Kafka...", flush=True)
            client = MongoClient(MONGO_URI)
            db = client["thesis_db"]
            collection = db["raw_user_logs"]

            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
            )
            redis_client.ping()
            print(
                f"[BOOT] Redis target host={REDIS_HOST} port={REDIS_PORT} db={REDIS_DB}",
                flush=True,
            )

            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_BROKER],
                group_id="data-processor-group",
                api_version=(0, 10, 1),
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            consumer.subscribe(pattern="^.*$")

            print("CONNECTED SUCCESSFUL!", flush=True)
            return collection, consumer, redis_client
        except Exception as error:
            print(f"Connection failed ({error}). Retrying in 5s...", flush=True)
            time.sleep(5)


collection, consumer, redis_client = connect_services()
print("Processor Started (Unwrap Logic Applied)...", flush=True)

for message in consumer:
    if message.topic.startswith("__"):
        continue

    raw_data = message.value
    log_data = {}

    if (
        isinstance(raw_data, dict)
        and "message" in raw_data
        and isinstance(raw_data["message"], dict)
    ):
        log_data = raw_data["message"]

        if "timestamp" not in log_data and "timestamp" in raw_data:
            log_data["timestamp"] = raw_data["timestamp"]
    else:
        log_data = raw_data

    log_data["kafka_topic_source"] = message.topic

    if filter_manager.is_noise(log_data):
        event_type = log_data.get("event_type", "Unknown")
        print(f"[FILTERED] {event_type} (Noise/Spam)", flush=True)
        continue

    try:
        collection.insert_one(log_data)
        update_realtime_recommendation_signals(redis_client, collection, log_data)
        print(
            f"[SAVED] {log_data.get('event_type')} | User: {log_data.get('user_id')}",
            flush=True,
        )
    except Exception as error:
        print(f"Error saving Mongo or realtime Redis: {error}", flush=True)
