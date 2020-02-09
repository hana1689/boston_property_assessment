import os
basedir = os.path.abspath(os.path.dirname(__file__))
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@ha-postgresql.cnhi406edexl.us-east-1.rds.amazonaws.com:5432/ha_db"

class Config(object):
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
    SQLALCHEMY_TRACK_MODIFICATIONS = False