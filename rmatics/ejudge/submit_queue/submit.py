import functools
from typing import Optional

from flask import current_app
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import joinedload

from rmatics import centrifugo_client
from rmatics.ejudge.ejudge_proxy import submit
from rmatics.ejudge.judges_config import get_judge, get_default_judge_id
from rmatics.model.base import db
from rmatics.model.run import Run
from rmatics.utils.functions import attrs_to_dict
from rmatics.utils.run import EjudgeStatuses


_REQUIRED_ENTRY_KEYS = ('contest_id', 'problem_id')


def _get_judge_entry(problem, lang_id: int, user_id: int) -> Optional[dict]:
    """Return the highest-priority matching judges_settings entry for (lang_id, user_id).

    judges_settings is a list of entries:
      {
        "judge_id":  <str>,    # optional — references a judge in judges.json
        "contest_id": <int>,   # required — contest_id inside that ejudge
        "problem_id": <int>,   # required — prob_id inside the contest
        "lang_ids":  [<int>],  # null / absent matches any language
        "user_ids":  [<int>]   # null / absent matches any moodle user
      }

    An entry is a candidate when BOTH filters match:
      - lang_ids is null  OR  lang_id  in lang_ids
      - user_ids is null  OR  user_id  in user_ids

    judges_settings entry shape:
      {
        "judge_id":  <int>,    # optional — references a judge in judges.json by numeric id
        "contest_id": <int>,   # required
        "problem_id": <int>,   # required
        "lang_ids":  [<int>],  # null / absent matches any language
        "user_ids":  [<int>]   # null / absent matches any moodle user
      }

    Entries missing contest_id or problem_id are skipped with a warning.
    Among valid candidates, higher specificity (more filters set) wins;
    listed order breaks ties. Returns None when no entry matches.
    """
    settings = problem.judges_settings
    if not settings:
        return None

    candidates = []
    for entry in settings:
        missing = [k for k in _REQUIRED_ENTRY_KEYS if k not in entry]
        if missing:
            current_app.logger.warning(
                f'Problem #{problem.id}: judges_settings entry missing required keys '
                f'{missing!r}, skipping: {entry!r}'
            )
            continue
        lang_ids = entry.get('lang_ids')
        user_ids = entry.get('user_ids')
        if (lang_ids is None or lang_id in lang_ids) and \
           (user_ids is None or user_id in user_ids):
            candidates.append(entry)

    if not candidates:
        return None

    candidates.sort(
        key=lambda e: -(
            (e.get('lang_ids') is not None) +
            (e.get('user_ids') is not None)
        )
    )
    return candidates[0]


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

    @retry_on_exception(sa_exc.OperationalError, times=4)
    def _get_run(self) -> Optional[Run]:
        run: Run = db.session.query(Run) \
            .options(joinedload(Run.problem)) \
            .get(self.run_id)

        return run

    @retry_on_exception(sa_exc.OperationalError, times=4)
    def _add_info_from_ejudge(self, run, ejudge_run_id,
                              judge_id, status: EjudgeStatuses):
        run.ejudge_status = status.value
        run.ejudge_run_id = ejudge_run_id
        run.judge_id = judge_id

        db.session.add(run)
        db.session.commit()

    @retry_on_exception(sa_exc.OperationalError, times=4)
    def _remove_run(self, run: Run):
        run.remove_source()
        db.session.delete(run)
        db.session.commit()

    def build_submit_error_protocol(self, ejudge_respone: str) -> dict:
        r = {
            'compiler_output': ejudge_respone,
            'run_id': self.run_id,
        }
        if len(ejudge_respone) > 0:
            r["ejResp"] = ejudge_respone
        return r

    def send(self, ejudge_url=None):
        default_url = ejudge_url or self.ejudge_url

        current_app.logger.info(f'Trying to send run #{self.run_id} to ejudge')

        run = self._get_run()
        if run is None:
            current_app.logger.error(f'Run #{self.run_id} is not found')
            return

        problem = run.problem
        if problem is None:
            current_app.logger.error(f'Run #{self.run_id}: problem not found')
            return
        db.session.expunge(problem)

        centrifugo_client.send_problem_run_updates(run.problem_id, run)

        entry = _get_judge_entry(problem, run.lang_id, run.user_id)

        if entry is not None:
            judge_id = entry.get('judge_id')
            contest_id = entry['contest_id']
            prob_id = entry['problem_id']
        else:
            judge_id = get_default_judge_id()
            contest_id = problem.ejudge_contest_id
            prob_id = problem.problem_id

        if judge_id:
            judge = get_judge(judge_id)
            if judge is None:
                current_app.logger.warning(
                    f'Run #{self.run_id}: judge_id {judge_id!r} not found in config, '
                    f'falling back to default URL'
                )
        else:
            judge = None

        entry_url = (judge.url if judge else None) or default_url
        entry_token = judge.get_token() if judge else None
        sender_user_id = judge.sender_user_id if judge else 5
        lang_id = judge.map_lang_id(run.lang_id) if judge else run.lang_id

        file = run.source

        try:
            ejudge_response = submit(
                run_file=file,
                contest_id=contest_id,
                prob_id=prob_id,
                lang_id=lang_id,
                filename='common_filename',
                url=entry_url,
                sender_user_id=sender_user_id,
                token=entry_token,
            )
        except Exception:
            current_app.logger.exception(
                f'Run #{self.run_id}: submit to judge {judge_id!r} raised exception'
            )
            return

        try:
            code = ejudge_response['code']
            if code != 0:
                raise ValueError(f'Ejudge returned status code {code}')
            ejudge_run_id = ejudge_response.get('run_id')
            self._add_info_from_ejudge(run, ejudge_run_id, judge_id, EjudgeStatuses(run.status))
            current_app.logger.info(f'Run #{self.run_id} successfully updated')
        except (TypeError, KeyError, ValueError):
            self._add_info_from_ejudge(run, None, judge_id, EjudgeStatuses.RMATICS_SUBMIT_ERROR)
            ejudge_compiler_output = ejudge_response.get('message', 'Ошибка отправки посылки')
            run.protocol = self.build_submit_error_protocol(ejudge_compiler_output)
            current_app.logger.error(f'Ejudge returned error for submit #{self.run_id}')

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
            ejudge_url=encoded['ejudge_url'],
        )

    def serialize(self, attributes=None):
        if attributes is None:
            attributes = (
                'id',
            )
        serialized = attrs_to_dict(self, *attributes)
        return serialized
