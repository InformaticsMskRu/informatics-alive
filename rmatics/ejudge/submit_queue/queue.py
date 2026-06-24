import pickle                                                                                                                                                                                                       

from .submit import Submit
from rmatics.model.base import redis
from rmatics.utils.redis.queue import RedisQueue
from flask import current_app

DEFAULT_SUBMIT_QUEUE = 'submit.queue'


def last_put_id_key(key):
    return f'{key}:last.put.id'


def last_get_id_key(key):
    return f'{key}:last.get.id'


def user_submits_key(key, user_id):
    return f'{key}:user:{user_id}'


class SubmitQueue(RedisQueue):
    """ 
    Очередь сабмитов.
    Кроме самих сабмитов поддерживает id последнего добавленного в очередь и
    id последнего полученного из очереди.
    """

    def __init__(self, key=DEFAULT_SUBMIT_QUEUE):
        super(SubmitQueue, self).__init__(key=key)

    def get_last_get_id(self):
        return int(redis.get(last_get_id_key(self.key)) or '0')

    def submit(self, run_id, ejudge_url):
        current_app.logger.info('Submit {} {}'.format(run_id, ejudge_url))
        # INCR и RPUSH атомарны сами по себе — WATCH/transaction здесь не нужен
        # (и ломался на redis-py >= 3.0: запись watched-ключей в immediate-режиме
        # давала вечный WatchError). Уникальность id обеспечивает INCR.
        submit = Submit(
            id=redis.incr(last_put_id_key(self.key)),
            run_id=run_id,
            ejudge_url=ejudge_url,
        )   
        self.put(submit.encode())
        return submit

    def get(self):
        # BLPOP атомарен; last_get_id — лишь счётчик прогресса, CAS не нужен.
        submit = Submit.decode(self.get_blocking())
        redis.set(last_get_id_key(self.key), submit.id)
        return submit
