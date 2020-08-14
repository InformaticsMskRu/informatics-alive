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
        def _submit(pipe):
            current_app.logger.info('_submit {} {} {} {}'.format(run_id, ejudge_url, self.key, last_put_id_key(self.key)))
            submit = Submit(
                id=pipe.incr(last_put_id_key(self.key)),
                run_id=run_id,
                ejudge_url=ejudge_url
            )
            current_app.logger.info('before put _submit {} {}'.format(submit, submit.encode()))
            self.put(submit.encode(), pipe=pipe)
            return submit
        submit = redis.transaction(
            _submit,
            self.key,
            last_get_id_key(self.key),
            last_put_id_key(self.key),
            value_from_callable=True
        )
        return submit

    def get(self):
        def _get(pipe):
            submit_encoded = super(SubmitQueue, self).get_blocking(pipe=pipe)
            current_app.logger.info('Here')
            submit = Submit.decode(submit_encoded)
            pipe.set(last_get_id_key(self.key), submit.id)
            return submit

        submit = redis.transaction(
            _get,
            self.key,
            last_get_id_key(self.key),
            last_put_id_key(self.key),
            value_from_callable=True,
        )

        return submit
