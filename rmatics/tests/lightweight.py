"""Lightweight local infrastructure for tests.

Runs the test suite without docker-compose (mariadb/mongo/redis):
  * MySQL   -> sqlite (the moodle/ejudge/pynformatics schemas are ATTACHed)
  * MongoDB -> mongomock
  * Redis   -> fakeredis

Enabled by the TEST_INFRA=local environment variable (see rmatics/testutils.py);
by default the tests use the real services from docker/docker-compose.yml.
"""
import os
import sys
import tempfile

try:
    import sqlite3
except ImportError:  # python built without sqlite3: use pysqlite3-binary
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

    # mysql-specific types sqlite doesn't know
    @compiles(MEDIUMTEXT, 'sqlite')
    def _compile_mediumtext(type_, compiler, **kw):
        return 'TEXT'

    tmpdir = tempfile.mkdtemp(prefix='rmatics-test-db-')

    # MySQL named locks: a name held by another connection is not waited
    # for, GET_LOCK returns 0 at once
    held_locks = {}

    def _named_lock_functions(dbapi_conn):
        def get_lock(name, timeout):
            if held_locks.get(name, dbapi_conn) is not dbapi_conn:
                return 0
            held_locks[name] = dbapi_conn
            return 1

        def release_lock(name):
            if held_locks.get(name) is not dbapi_conn:
                return None if name not in held_locks else 0
            del held_locks[name]
            return 1

        dbapi_conn.create_function('GET_LOCK', 2, get_lock)
        dbapi_conn.create_function('RELEASE_LOCK', 1, release_lock)

    @event.listens_for(Engine, 'connect')
    def _attach_schemas(dbapi_conn, connection_record):
        if not isinstance(dbapi_conn, sqlite3.Connection):
            return
        cursor = dbapi_conn.cursor()
        for schema in SQLITE_SCHEMAS:
            path = os.path.join(tmpdir, f'{schema}.db')
            cursor.execute(f'ATTACH DATABASE ? AS {schema}', (path,))
        cursor.close()
        _named_lock_functions(dbapi_conn)

    from rmatics.config import TestConfig
    TestConfig.SQLALCHEMY_DATABASE_URI = \
        'sqlite:///' + os.path.join(tmpdir, 'main.db')
    # pool options don't apply to sqlite (NullPool)
    TestConfig.SQLALCHEMY_POOL_SIZE = None
    TestConfig.SQLALCHEMY_POOL_RECYCLE = None


def _patch_redis():
    """All redis clients (flask_redis, redlock) are created with
    StrictRedis.from_url: it is replaced with fakeredis on a shared server."""
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
