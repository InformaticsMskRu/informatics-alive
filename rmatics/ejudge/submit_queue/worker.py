from flask import current_app
from gevent import Greenlet, sleep
from sqlalchemy import exc as sa_exc

from rmatics.model.base import db


class SubmitWorker(Greenlet):
    def __init__(self, queue):
        super(SubmitWorker, self).__init__()
        self.queue = queue
        self._ctx = current_app.app_context()
        self.ejudge_url = current_app.config['EJUDGE_NEW_CLIENT_URL']

    def handle_submit(self):
        submit = self.queue.get()
        try:
            submit.send(ejudge_url=self.ejudge_url)
        except sa_exc.OperationalError:
            current_app.logger.exception('Something was wrong with MySQL')
            raise
        except Exception:
            current_app.logger.exception('Submit worker caught exception and skipped submit without notifying user')

        finally:
            # handle_submit вызывается внутри контекста;
            # rollback помогает избегать ошибок с незакрытыми транзакциями
            db.session.rollback()

    def _run(self):
        while True:
            try:
                with self._ctx:
                    current_app.logger.info('Worker started')
                    while True:
                        self.handle_submit()
            except sa_exc.OperationalError:
                current_app.logger.warning('Something was wrong with MySQL; trying to restart worker')
                sleep(1)
