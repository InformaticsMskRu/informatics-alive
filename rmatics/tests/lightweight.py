"""Лёгкая локальная инфраструктура для тестов.

Позволяет запускать тестовый набор без docker-compose (mariadb/mongo/redis):
  * MySQL   -> sqlite (схемы moodle/ejudge/pynformatics цепляются через ATTACH)
  * MongoDB -> mongomock
  * Redis   -> fakeredis

Включается переменной окружения TEST_INFRA=local (см. rmatics/testutils.py),
по умолчанию тесты, как и раньше, ходят в настоящие сервисы из
docker/docker-compose.yml.
"""
import os
import sys
import tempfile

try:
    import sqlite3
except ImportError:  # интерпретатор собран без sqlite3 — берём pysqlite3-binary
    import pysqlite3 as sqlite3
    sys.modules['sqlite3'] = sqlite3
    sys.modules['sqlite3.dbapi2'] = sqlite3.dbapi2

_enabled = False

SQLITE_SCHEMAS = ('moodle', 'ejudge', 'pynformatics')


def enable():
    global _enabled
    if _enabled:
        return
    _enabled = True

    _patch_sqlalchemy()
    _patch_redis()
    _patch_mongo()


def _patch_sqlalchemy():
    from sqlalchemy import event
    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.dialects.mysql import MEDIUMTEXT

    # mysql-специфичные типы, которых sqlite не знает
    @compiles(MEDIUMTEXT, 'sqlite')
    def _compile_mediumtext(type_, compiler, **kw):
        return 'TEXT'

    tmpdir = tempfile.mkdtemp(prefix='rmatics-test-db-')

    @event.listens_for(Engine, 'connect')
    def _attach_schemas(dbapi_conn, connection_record):
        if not isinstance(dbapi_conn, sqlite3.Connection):
            return
        cursor = dbapi_conn.cursor()
        for schema in SQLITE_SCHEMAS:
            path = os.path.join(tmpdir, f'{schema}.db')
            cursor.execute(f'ATTACH DATABASE ? AS {schema}', (path,))
        cursor.close()

    from rmatics.config import TestConfig
    TestConfig.SQLALCHEMY_DATABASE_URI = \
        'sqlite:///' + os.path.join(tmpdir, 'main.db')
    # опции пула несовместимы с sqlite (NullPool)
    TestConfig.SQLALCHEMY_POOL_SIZE = None
    TestConfig.SQLALCHEMY_POOL_RECYCLE = None


def _patch_redis():
    """Все клиенты redis (flask_redis, redlock) создаются через
    StrictRedis.from_url — подменяем его на fakeredis с общим сервером."""
    import fakeredis
    import redis as redis_pkg

    server = fakeredis.FakeServer()

    def _fake_from_url(cls, url, **kwargs):
        kwargs.pop('server', None)
        return fakeredis.FakeStrictRedis(server=server)

    redis_pkg.StrictRedis.from_url = classmethod(_fake_from_url)
    redis_pkg.Redis.from_url = classmethod(_fake_from_url)


def _patch_mongo():
    import mongomock
    import flask_pymongo

    client = mongomock.MongoClient()

    def _init_app(self, app, uri=None, *args, **kwargs):
        self.cx = client
        self.db = client['test']

    flask_pymongo.PyMongo.init_app = _init_app
