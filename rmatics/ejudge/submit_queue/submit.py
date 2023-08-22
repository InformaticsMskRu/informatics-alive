import functools
from typing import Optional

from flask import current_app
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import joinedload

from rmatics import centrifugo_client
from rmatics.ejudge.ejudge_proxy import submit
from rmatics.model.base import db
from rmatics.model.run import Run
from rmatics.utils.functions import attrs_to_dict
from rmatics.utils.run import EjudgeStatuses


def retry_on_exception(exception_class: Exception, times=5):
    times += 1

    def wrapper(func):
        @functools.wraps(func)
        def retryer(*args, **kwargs):
            last_exc = ValueError('Parameter times should be positive')
            for counter in range(times):
                try:
                    return func(*args, **kwargs)
                except exception_class as e:
                    last_exc = e
            raise last_exc

        return retryer

    return wrapper


class Submit:
    def __init__(self, id, run_id: int, ejudge_url: str):
        self.id = id
        self.run_id = run_id
        self.ejudge_url = ejudge_url
        self.ejudge_user = current_app.config.get('EJUDGE_USER')
        self.ejudge_password = current_app.config.get('EJUDGE_PASSWORD')

    @retry_on_exception(sa_exc.OperationalError, times=4)
    def _get_run(self) -> Optional[Run]:
        run: Run = db.session.query(Run) \
            .options(joinedload(Run.problem)) \
            .get(self.run_id)

        return run

    @retry_on_exception(sa_exc.OperationalError, times=4)
    def _add_info_from_ejudge(self, run, ejudge_run_id,
                              ejudge_url, status: EjudgeStatuses):
        run.ejudge_status = status.value
        run.ejudge_run_id = ejudge_run_id
        run.ejudge_url = ejudge_url

        db.session.add(run)
        db.session.commit()

    @retry_on_exception(sa_exc.OperationalError, times=4)
    def _remove_run(self, run: Run):
        run.remove_source()
        db.session.delete(run)
        db.session.commit()

    def build_submit_error_protocol(self, ejudge_respone: str) -> dict:
        """Generate protocol for invalid submission, which can be inserted to mongo and served to client

        :return: Protocol for invalid submition
        """

        r = {
            'compiler_output': ejudge_respone,
            'run_id': self.run_id,
        }
        if len(ejudge_respone) > 0:
            r["ejResp"] = ejudge_respone
        return r

    def send(self, ejudge_url=None):
        current_app.logger.info(f'Trying to send run #{self.run_id} to ejudge')

        ejudge_url = ejudge_url or self.ejudge_url

        run = self._get_run()

        if run is None:
            current_app.logger.error(f'Run #{self.run_id} is not found')
            return

        problem = run.problem
        db.session.expunge(problem)

        ejudge_language_id = run.ejudge_language_id

        file = run.source

        centrifugo_client.send_problem_run_updates(run.problem_id, run)

        try:
            ejudge_response = submit(
                run_file=file,
                contest_id=problem.ejudge_contest_id,
                prob_id=problem.problem_id,
                lang_id=ejudge_language_id,
                login=self.ejudge_user,
                password=self.ejudge_password,
                filename='common_filename',
                url=ejudge_url,
            )
        except Exception:
            current_app.logger.exception('Unknown Ejudge submit error')
            return

        try:
            code = ejudge_response['code']
            if code != 0:
                raise ValueError(f'Ejudge returned status code {code}')
            ejudge_run_id = ejudge_response.get('run_id')
            self._add_info_from_ejudge(run, ejudge_run_id, ejudge_url, EjudgeStatuses(run.status))
            current_app.logger.info(f'Run #{self.run_id} successfully updated')
        except (TypeError, KeyError, ValueError):
            # If Ejudge can't process submit, set generic error code for run
            self._add_info_from_ejudge(run, None, ejudge_url, EjudgeStatuses.RMATICS_SUBMIT_ERROR)

            # Proxy actual ejudge output to generic template protocol for client
            ejudge_compiler_output = ejudge_response.get('message', 'Ошибка отправки посылки')
            run.protocol = self.build_submit_error_protocol(ejudge_compiler_output)

            current_app.logger.error(f'Ejudge retunred error for submit #{self.run_id}')

    def encode(self):
        return {
            'id': self.id,
            'run_id': self.run_id,
            'ejudge_url': self.ejudge_url,
        }

    @staticmethod
    def decode(encoded):
        return Submit(
            id=encoded['id'],
            run_id=encoded['run_id'],
            ejudge_url=encoded['ejudge_url']
        )

    def serialize(self, attributes=None):
        if attributes is None:
            attributes = (
                'id',
            )
        serialized = attrs_to_dict(self, *attributes)
        return serialized
