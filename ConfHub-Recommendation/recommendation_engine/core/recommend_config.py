import json
import os
from copy import deepcopy

import requests
from psycopg2.extras import RealDictCursor

from core.db_client import get_postgres_connection, get_redis_connection


RECOMMEND_CONFIG_API_URL = os.getenv(
    "RECOMMEND_CONFIG_API_URL",
    "http://localhost:3000/api/v1/admin/recommend-config"
)

REDIS_CONFIG_KEYS = (
    "recommend_config",
    "recommend-config",
    "confhub:recommend_config",
    "confhub:recommend-config",
)

DEFAULT_RECOMMEND_CONFIG = {
    "shortTermEventWeights": {
        "CLICK_FOLLOW_STAR": 3.0,
        "CLICK_ADD_CALENDAR": 2.8,
        "CLICK_EXTERNAL_WEBSITE": 2.5,
        "CLICK_CONFERENCE_TITLE": 2.0,
        "CLICK_RELATED_DETAIL": 2.0,
        "VIEW_CONFERENCE_DETAIL": 1.8,
        "SUBMIT_FEEDBACK": 2.0,
        "TIME_ON_PAGE": 1.2,
        "SCROLL_DEPTH": 1.0,
        "SEARCH_CONFERENCE": 3.5,
    },
    "recentTrackingLimit": 60,
    "topKNeighbors": 10,
    "activeCandidateLimit": 1000,
    "finalRecommendationLimit": 50,
    "forYouLimit": 50,
    "newUserCandidateLimit": 50,
    "trendingCandidateLimit": 50,
    "relatedConferencesLimit": 50,
    "detailRecommendationLimit": 10,
    "cfScoreWeight": 2.5,
    "interactedPenalty": 5.0,
    "negativeItemPenalty": 0.5,
    "behaviorTopicDays": 7,
    "behaviorTopK": 3,
    "longTermTopicLimit": 10,
    "searchIntentWeight": 0.35,
    "searchBoostWeight": 0.25,
    "searchHistoryDays": 2,
    "smartSearchCandidateLimit": 50,
    "smartSearchCfWeight": 2.0,
    "offlineCfNeighborLimit": 10,
    "offlineOwnRatingWeight": 2.0,
    "offlineCfTtlSeconds": 86400,
    "modelToRun": "all",
    "cronEnabled": True,
    "cronHour": 2,
    "cronMinute": 0,
    "cronModelToRun": "all",
    "cronRefreshSeconds": 60,
}


EVENT_WEIGHT_KEY_MAP = {
    "clickFollowStar": "CLICK_FOLLOW_STAR",
    "clickAddCalendar": "CLICK_ADD_CALENDAR",
    "clickExternalWebsite": "CLICK_EXTERNAL_WEBSITE",
    "clickConferenceTitle": "CLICK_CONFERENCE_TITLE",
    "clickRelatedDetail": "CLICK_RELATED_DETAIL",
    "viewConferenceDetail": "VIEW_CONFERENCE_DETAIL",
    "submitFeedback": "SUBMIT_FEEDBACK",
    "timeOnPage": "TIME_ON_PAGE",
    "scrollDepth": "SCROLL_DEPTH",
    "searchConference": "SEARCH_CONFERENCE",
}


def _deep_merge(base, override):
    merged = deepcopy(base)
    if not isinstance(override, dict):
        return merged

    for key, value in override.items():
        if value is None:
            continue
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _normalize_backend_config(raw_config):
    if not isinstance(raw_config, dict):
        return {}

    normalized = {}

    short_term_event_weights = {}
    for weights_key in ("eventWeights", "behaviorWeights"):
        weights = raw_config.get(weights_key)
        if isinstance(weights, dict):
            short_term_event_weights.update({
                EVENT_WEIGHT_KEY_MAP.get(key, key): value
                for key, value in weights.items()
                if value is not None
            })

    if short_term_event_weights:
        normalized["shortTermEventWeights"] = short_term_event_weights

    hybrid_weights = raw_config.get("hybridWeights")
    if isinstance(hybrid_weights, dict):
        if hybrid_weights.get("cfScoreMultiplier") is not None:
            normalized["cfScoreWeight"] = hybrid_weights["cfScoreMultiplier"]
        if hybrid_weights.get("interactedPenalty") is not None:
            normalized["interactedPenalty"] = hybrid_weights["interactedPenalty"]
        if hybrid_weights.get("negativeItemsPenalty") is not None:
            normalized["negativeItemPenalty"] = hybrid_weights["negativeItemsPenalty"]

    direct_key_map = {
        "trackingLimit": "recentTrackingLimit",
        "topKNeighbors": "topKNeighbors",
        "candidateLimit": "activeCandidateLimit",
        "forYouLimit": "forYouLimit",
        "filterLimit": "finalRecommendationLimit",
        "searchIntentWeight": "searchIntentWeight",
        "boostScoreMultiplier": "searchBoostWeight",
        "keywordHistoryDays": "searchHistoryDays",
        "trendingLimit": "trendingCandidateLimit",
        "relatedConferencesLimit": "relatedConferencesLimit",
        "detailRecommendationLimit": "detailRecommendationLimit",
        "behaviorTopicDays": "behaviorTopicDays",
        "behaviorTopK": "behaviorTopK",
        "longTermTopicLimit": "longTermTopicLimit",
        "smartSearchCandidateLimit": "smartSearchCandidateLimit",
        "smartSearchCfWeight": "smartSearchCfWeight",
        "offlineCfNeighborLimit": "offlineCfNeighborLimit",
        "modelToRun": "modelToRun",
        "selectedModel": "modelToRun",
        "recommendationModel": "modelToRun",
        "jobTarget": "modelToRun",
        "cronEnabled": "cronEnabled",
        "scheduleEnabled": "cronEnabled",
        "enableCron": "cronEnabled",
        "cronHour": "cronHour",
        "scheduleHour": "cronHour",
        "cronMinute": "cronMinute",
        "scheduleMinute": "cronMinute",
        "cronModelToRun": "cronModelToRun",
        "scheduleModelToRun": "cronModelToRun",
        "cronRefreshSeconds": "cronRefreshSeconds",
    }

    for source_key, target_key in direct_key_map.items():
        if raw_config.get(source_key) is not None:
            normalized[target_key] = raw_config[source_key]

    if normalized.get("forYouLimit") is not None:
        normalized["finalRecommendationLimit"] = normalized["forYouLimit"]
    elif normalized.get("finalRecommendationLimit") is not None:
        normalized["forYouLimit"] = normalized["finalRecommendationLimit"]

    return normalized


def _load_config_from_api():
    try:
        response = requests.get(RECOMMEND_CONFIG_API_URL, timeout=5)
        response.raise_for_status()
        config = response.json()
        print(f"[CONFIG] Loaded recommendation config from API: {RECOMMEND_CONFIG_API_URL}")
        return _normalize_backend_config(config)
    except Exception as e:
        print(f"[CONFIG] Could not load recommendation config from API: {e}")

    return {}


def _load_config_from_redis():
    try:
        redis_client = get_redis_connection()
        for key in REDIS_CONFIG_KEYS:
            raw_value = redis_client.get(key)
            if not raw_value:
                continue

            config = json.loads(raw_value)
            if isinstance(config, dict):
                print(f"[CONFIG] Loaded recommendation config from Redis key: {key}")
                return _normalize_backend_config(config) or config
    except Exception as e:
        print(f"[CONFIG] Could not load recommendation config from Redis: {e}")

    return {}


def _load_config_from_postgres():
    try:
        conn = get_postgres_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT * FROM "RecommendConfig" LIMIT 1')
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            print("[CONFIG] Loaded recommendation config from Postgres RecommendConfig")
            config = dict(row)
            return _normalize_backend_config(config) or config
    except Exception as e:
        print(f"[CONFIG] Could not load recommendation config from Postgres: {e}")

    return {}


def get_recommend_config():
    config = deepcopy(DEFAULT_RECOMMEND_CONFIG)
    config = _deep_merge(config, _load_config_from_postgres())
    config = _deep_merge(config, _load_config_from_redis())
    config = _deep_merge(config, _load_config_from_api())
    return config


def get_short_term_event_weights(config=None):
    config = config or get_recommend_config()
    return config.get("shortTermEventWeights", DEFAULT_RECOMMEND_CONFIG["shortTermEventWeights"])


def normalize_model_to_run(model_to_run):
    if not model_to_run:
        return "all"

    value = str(model_to_run).strip().lower().replace("-", "_")
    aliases = {
        "all": "all",
        "both": "all",
        "full": "all",
        "personalized": "personalized",
        "personalization": "personalized",
        "user_based": "personalized",
        "user_based_cf": "personalized",
        "recommend_user": "personalized",
        "search": "search_rerank",
        "search_rerank": "search_rerank",
        "search_reranking": "search_rerank",
        "precompute": "search_rerank",
        "precompute_cf": "search_rerank",
        "cf_scores": "search_rerank",
        "hash": "search_rerank",
        "item": "item_based",
        "item_based": "item_based",
        "item_based_cf": "item_based",
        "related": "item_based",
        "related_conferences": "item_based",
        "detail": "item_based",
    }
    return aliases.get(value, "all")


def log_recommend_config(config, title="Recommendation Config"):
    print("\n" + "=" * 72, flush=True)
    print(f"[CONFIG] {title}", flush=True)
    print("-" * 72, flush=True)
    print("[CONFIG] Event weights:", flush=True)
    for event_name, weight in sorted(config.get("shortTermEventWeights", {}).items()):
        print(f"[CONFIG]   {event_name}: {weight}", flush=True)

    print("[CONFIG] Limits:", flush=True)
    print(f"[CONFIG]   recentTrackingLimit: {config.get('recentTrackingLimit')}", flush=True)
    print(f"[CONFIG]   topKNeighbors: {config.get('topKNeighbors')}", flush=True)
    print(f"[CONFIG]   activeCandidateLimit: {config.get('activeCandidateLimit')}", flush=True)
    print(f"[CONFIG]   finalRecommendationLimit/forYouLimit: {config.get('finalRecommendationLimit')}", flush=True)
    print(f"[CONFIG]   newUserCandidateLimit: {config.get('newUserCandidateLimit')}", flush=True)
    print(f"[CONFIG]   trendingCandidateLimit: {config.get('trendingCandidateLimit')}", flush=True)
    print(f"[CONFIG]   relatedConferencesLimit: {config.get('relatedConferencesLimit')}", flush=True)
    print(f"[CONFIG]   detailRecommendationLimit: {config.get('detailRecommendationLimit')}", flush=True)

    print("[CONFIG] Hybrid weights:", flush=True)
    print(f"[CONFIG]   cfScoreWeight: {config.get('cfScoreWeight')}", flush=True)
    print(f"[CONFIG]   interactedPenalty: {config.get('interactedPenalty')}", flush=True)
    print(f"[CONFIG]   negativeItemPenalty: {config.get('negativeItemPenalty')}", flush=True)

    print("[CONFIG] Search/topic:", flush=True)
    print(f"[CONFIG]   searchIntentWeight: {config.get('searchIntentWeight')}", flush=True)
    print(f"[CONFIG]   searchBoostWeight/boostScoreMultiplier: {config.get('searchBoostWeight')}", flush=True)
    print(f"[CONFIG]   searchHistoryDays/keywordHistoryDays: {config.get('searchHistoryDays')}", flush=True)
    print(f"[CONFIG]   behaviorTopicDays: {config.get('behaviorTopicDays')}", flush=True)
    print(f"[CONFIG]   behaviorTopK: {config.get('behaviorTopK')}", flush=True)
    print(f"[CONFIG]   longTermTopicLimit: {config.get('longTermTopicLimit')}", flush=True)
    print(f"[CONFIG]   smartSearchCandidateLimit: {config.get('smartSearchCandidateLimit')}", flush=True)
    print(f"[CONFIG]   smartSearchCfWeight: {config.get('smartSearchCfWeight')}", flush=True)
    print(f"[CONFIG]   offlineCfNeighborLimit: {config.get('offlineCfNeighborLimit')}", flush=True)
    print("[CONFIG] Model selection:", flush=True)
    print(f"[CONFIG]   modelToRun: {normalize_model_to_run(config.get('modelToRun'))}", flush=True)
    print("[CONFIG] Cron:", flush=True)
    print(f"[CONFIG]   cronEnabled: {config.get('cronEnabled')}", flush=True)
    print(f"[CONFIG]   cronHour: {config.get('cronHour')}", flush=True)
    print(f"[CONFIG]   cronMinute: {config.get('cronMinute')}", flush=True)
    print(f"[CONFIG]   cronModelToRun: {normalize_model_to_run(config.get('cronModelToRun'))}", flush=True)
    print(f"[CONFIG]   cronRefreshSeconds: {config.get('cronRefreshSeconds')}", flush=True)
    print("=" * 72 + "\n", flush=True)
