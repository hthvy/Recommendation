import os
import json

from apscheduler.schedulers.background import BackgroundScheduler
from core.db_client import REDIS_DB, REDIS_HOST, REDIS_PORT, get_redis_connection
from core.recommend_config import get_recommend_config, normalize_model_to_run
from main_job import ai_pipeline_job


DEFAULT_CHANNELS = "ai_model_channel,confhub:ai_model_channel"
REDIS_CHANNELS = [
    channel.strip()
    for channel in os.getenv("AI_MODEL_CHANNELS", DEFAULT_CHANNELS).split(",")
    if channel.strip()
]
RETRAIN_COMMAND = os.getenv("AI_RETRAIN_COMMAND", "START_RETRAIN")
AI_PIPELINE_CRON_JOB_ID = "ai_pipeline_cron"
AI_PIPELINE_CRON_REFRESH_JOB_ID = "ai_pipeline_cron_refresh"


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on", "enabled")


def _as_int(value, default, min_value=None, max_value=None):
    try:
        parsed = int(value)
    except Exception:
        parsed = default

    if min_value is not None:
        parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def sync_ai_cron_from_admin(scheduler):
    config = get_recommend_config()
    cron_enabled = _as_bool(config.get("cronEnabled"), default=True)
    cron_hour = _as_int(config.get("cronHour"), 2, min_value=0, max_value=23)
    cron_minute = _as_int(config.get("cronMinute"), 0, min_value=0, max_value=59)
    cron_model = normalize_model_to_run(config.get("cronModelToRun") or config.get("modelToRun"))

    if cron_enabled:
        scheduler.add_job(
            ai_pipeline_job,
            "cron",
            hour=cron_hour,
            minute=cron_minute,
            args=[cron_model],
            id=AI_PIPELINE_CRON_JOB_ID,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        print(
            "[CRON] Enabled -> "
            f"{cron_hour:02d}:{cron_minute:02d}, model={cron_model}",
            flush=True,
        )
    else:
        if scheduler.get_job(AI_PIPELINE_CRON_JOB_ID):
            scheduler.remove_job(AI_PIPELINE_CRON_JOB_ID)
        print("[CRON] Disabled by admin config", flush=True)


def start_ai_scheduler():
    scheduler = BackgroundScheduler()
    config = get_recommend_config()
    cron_refresh_seconds = _as_int(config.get("cronRefreshSeconds"), 60, min_value=15)

    sync_ai_cron_from_admin(scheduler)
    scheduler.add_job(
        lambda: sync_ai_cron_from_admin(scheduler),
        "interval",
        seconds=cron_refresh_seconds,
        id=AI_PIPELINE_CRON_REFRESH_JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    print(
        "[CRON] Scheduler started. "
        f"Admin config refresh interval: {cron_refresh_seconds}s",
        flush=True,
    )
    return scheduler


def parse_admin_command(raw_command):
    command_text = str(raw_command).strip()
    model_to_run = None

    try:
        payload = json.loads(command_text)
        if isinstance(payload, dict):
            command_text = str(
                payload.get("command")
                or payload.get("type")
                or payload.get("event")
                or RETRAIN_COMMAND
            ).strip()
            model_to_run = (
                payload.get("model")
                or payload.get("modelToRun")
                or payload.get("selectedModel")
                or payload.get("cronModelToRun")
                or payload.get("jobTarget")
            )
    except Exception:
        pass

    if ":" in command_text:
        command_name, model_name = command_text.split(":", 1)
        command_text = command_name.strip()
        model_to_run = model_to_run or model_name.strip()

    return command_text, model_to_run


def listen_for_admin_commands():
    """
    Listen for admin commands via Redis Pub/Sub.
    When the backend publishes START_RETRAIN, run the recommendation pipeline.
    """
    redis_client = get_redis_connection()
    redis_client.ping()
    scheduler = start_ai_scheduler()

    pubsub = redis_client.pubsub()
    pubsub.subscribe(*REDIS_CHANNELS)

    print("Listening for admin commands from Redis...")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}, db={REDIS_DB}")
    print(f"Channels: {', '.join(REDIS_CHANNELS)}")
    print(f"Retrain command: {RETRAIN_COMMAND}")
    print("=" * 50)

    try:
        for message in pubsub.listen():
            if message["type"] != "message":
                continue

            raw_command = message["data"]
            command, model_to_run = parse_admin_command(raw_command)
            channel = message.get("channel")
            print(f"\nReceived command on {channel}: {raw_command}")
            print(f"Parsed command: {command}, model: {model_to_run or 'from-config'}")

            if command != RETRAIN_COMMAND:
                print(f"Unknown command: {command}")
                continue

            print("Starting recommendation pipeline...")
            print("=" * 50)
            try:
                ai_pipeline_job(model_to_run=model_to_run)
                print("Recommendation pipeline completed.")
            except Exception as e:
                print(f"Recommendation pipeline failed: {e}")
            print("=" * 50)
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    listen_for_admin_commands()
