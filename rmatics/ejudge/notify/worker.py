from flask import current_app
from gevent import Greenlet, sleep
from sqlalchemy import exc as sa_exc

from rmatics.model.base import db

from .queue import NotifyQueue

class NotifyWorker(Greenlet):
    def __init__(self, worker_id):
        super(NotifyWorker, self).__init__()
        self._ctx = current_app.app_context()
        self.id = worker_id
        self.queue = None

    def handle_submit(self):
        current_app.logger.info('Try get from queue')
        try:
            submit = self.queue.get_and_process()
            current_app.logger.info('Got and processed!')
        except sa_exc.OperationalError:
            current_app.logger.exception('Something was wrong with MySQL')
            raise
        except Exception:
            current_app.logger.exception('Notify worker caught exception and skipped submit without notifying user')

        finally:
            # handle_submit вызывается внутри контекста;
            # rollback помогает избегать ошибок с незакрытыми транзакциями
            db.session.rollback()

    def _run(self):
        while True:
            try:
                with self._ctx:
                    stream = current_app.config['EJUDGE_NOTIFY_STREAM']
                    group = current_app.config['EJUDGE_NOTIFY_GROUP']
                    consumer = f"{group}.{self.id}"

                    self.queue = NotifyQueue(stream, group, consumer)

                    current_app.logger.info('Worker started')
                    while True:
                        self.handle_submit()
            except sa_exc.OperationalError:
                with self._ctx:
                    current_app.logger.warning('Something was wrong with MySQL; trying to restart worker')
                sleep(1)
