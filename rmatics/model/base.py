from flask_pymongo import PyMongo
from flask_redis import FlaskRedis
from flask_sqlalchemy import SQLAlchemy
from celery import Celery

from rmatics.utils.patched_packages.patched_pymongo import patch_pymongo_to_avoiding_deadlocks
patch_pymongo_to_avoiding_deadlocks()

db = SQLAlchemy()
mongo = PyMongo()
redis = FlaskRedis()
celery = Celery('rmatics-alive')