from pymongo import MongoClient
from app.conf import app_config


def get_mongo_client():
    db_host = app_config.MONGODB_HOST
    db_port = app_config.MONGODB_PORT
    client = MongoClient(db_host, db_port)
    return client
