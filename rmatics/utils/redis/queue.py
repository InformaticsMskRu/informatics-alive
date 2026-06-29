import pickle

from rmatics.model.base import redis
from flask import current_app


class RedisQueue:
    def __init__(self, key):
        self.key = key

    def put(self, value, pipe=None):
        if pipe is None:
            pipe = redis
        current_app.logger.info('rpush {} {}'.format(self.key, pickle.dumps(value)))
        pipe.rpush(self.key, pickle.dumps(value))

    def get(self, pipe=None):
        if pipe is None:
            pipe = redis

        value = pipe.lpop(self.key)
        if value:
            value = pickle.loads(value)
        return value

    def get_blocking(self, timeout=0, pipe=None):
        if pipe is None:
            pipe = redis
        value = pipe.blpop(self.key, timeout=timeout)
        if value:
            value = pickle.loads(value[1])
        return value


class RedisStreamsQueue:
    def __init__(self, streams, group, consumer):
        self.streams = streams
        self.group = group
        self.consumer = consumer

        self._ensure_group()

    def _ensure_group(self):
        for stream in self.streams:
            try:
                redis.xgroup_create(
                    name=stream,
                    groupname=self.group,
                    id="0",
                    mkstream=True
                )

                current_app.logger.info(
                    f"created redis stream group {self.group} on {stream}"
                )
            except Exception as e:
                # BUSYGROUP → группа уже существует
                if "BUSYGROUP" not in str(e):
                    raise

    def get(self):
        if len(self.streams) == 0:
            current_app.logger.error("no streams provided")
            raise RuntimeError
        resp = redis.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={stream: ">" for stream in self.streams},
            block=0
        )

        return resp

    def get_blocking(self, timeout=0):
        if len(self.streams) == 0:
            current_app.logger.error("no streams provided")
            exit(123)
        resp = redis.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={stream: ">" for stream in self.streams},
            block=timeout * 1000 if timeout else 0
        )

        return resp

    def ack(self, stream, message_id):
        redis.xack(stream, self.group, message_id)
