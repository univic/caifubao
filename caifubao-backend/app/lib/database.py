
from app import app_config
from flask_mongoengine import MongoEngine


db = MongoEngine()


def db_init(app):
    db.init_app(app)
