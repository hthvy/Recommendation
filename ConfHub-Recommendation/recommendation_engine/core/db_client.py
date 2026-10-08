import os

from pymongo import MongoClient
import psycopg2
import redis
import json

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "thesis_db")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "confhub")
POSTGRES_USER = os.getenv("POSTGRES_USER", "confhub")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))
# REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

def get_mongodb_connection():
    client = MongoClient(MONGO_URI)
    # CHỈ trả về database, không trả về collection ở đây
    return client[MONGO_DB]

def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )

def get_redis_connection():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )

def save_to_redis(redis_client, key: str, data_list: list):
    json_data = json.dumps(data_list)
    redis_client.set(key, json_data)
    print(f"📦 Đã lưu vào Redis key: {key}")
