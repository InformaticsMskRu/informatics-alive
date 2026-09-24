from celery import shared_task
from celery.utils.log import get_task_logger

from typing import Optional
from rmatics.model import Run
from rmatics.model.base import db
from sqlalchemy.orm import joinedload
from rmatics.utils.run import EjudgeStatuses

from rmatics.ejudge.judges_config import get_judge
from rmatics.ejudge.routing import (  # noqa: F401 (_get_judge_entry is imported by tests)
    LANGUAGE_NOT_AVAILABLE_MESSAGE,
    LanguageNotAvailable,
    _get_judge_entry,
    resolve_route,
)
from rmatics.ejudge.ejudge_proxy import submit

from rmatics import centrifugo_client

logger = get_task_logger(__name__)

def _get_run(run_id) -> Optional[Run]:
    run: Run = db.session.query(Run) \
        .options(joinedload(Run.problem)) \
        .get(run_id)

    return run

def _add_info_from_ejudge(run, ejudge_run_id, ejudge_run_uuid, status, judge_id):
    if status is not None:
        run.ejudge_status = status.value
    run.ejudge_run_id = ejudge_run_id
    run.ejudge_run_uuid = ejudge_run_uuid
    run.judge_id = judge_id

    db.session.add(run)
    db.session.commit()

def _build_submit_error_protocol(run_id, ejudge_respone: str) -> dict:
    r = {
        'compiler_output': ejudge_respone,
        'run_id': run_id,
    }
    if len(ejudge_respone) > 0:
        r["ejResp"] = ejudge_respone
    return r

@shared_task(name='rmatics.ejudge.submit_queue.task.submit_task', ignore_result=True, bind=True, default_retry_delay=5, max_retries=5)
def submit_task(self, run_id):
    logger.info(f'Trying to send run #{run_id} to ejudge')

    run = _get_run(run_id)
    if run is None:
        logger.error(f'Run #{run_id} is not found')
        return

    problem = run.problem
    if problem is None:
        logger.error(f'Run #{run_id}: problem not found')
        return
    db.session.expunge(problem)

    centrifugo_client.send_problem_run_updates(run.problem_id, run)

    try:
        judge_id, contest_id, prob_id = resolve_route(problem, run.lang_id, run.user_id)
    except LanguageNotAvailable as e:
        # judges_settings may have changed since the submit was accepted
        # (or the run is being rejudged)
        logger.error(f'Run #{run_id}: {e}')
        _add_info_from_ejudge(run, None, None, EjudgeStatuses.RMATICS_SUBMIT_ERROR, None)
        run.protocol = _build_submit_error_protocol(run_id, LANGUAGE_NOT_AVAILABLE_MESSAGE)
        return

    if judge_id is None:
        logger.error(
            'Neither judge_id nor DEFAULT_JUDGE_ID were not found'
        )
        return

    judge = get_judge(judge_id)
    if judge is None:
        logger.error(
            f'Run #{run_id}: judge_id {judge_id!r} not found in config'
        )
        return

    entry_url = judge.url
    entry_token = judge.get_token()
    sender_user_id = judge.sender_user_id
    lang_id = judge.map_lang_id(run.lang_id)

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
            ext_user_id=run_id
        )
    except Exception as e:
        logger.error(
            f'Run #{run_id}: submit to judge {judge_id!r} raised exception'
        )
        if self.request.retries < submit_task.max_retries:
            logger.info('retry submit')
            self.retry(exc=e)

        _add_info_from_ejudge(run, None, None, EjudgeStatuses.RMATICS_SUBMIT_ERROR, judge_id)
        run.protocol = _build_submit_error_protocol(run_id, 'Ошибка отправки посылки')
        logger.error('submit failed after 3 retries')
        return


    try:
        code = ejudge_response['code']
        if code != 0:
            raise ValueError(f'Ejudge returned status code {code}')
        ejudge_run_id = ejudge_response.get('run_id')
        ejudge_run_uuid = ejudge_response.get('run_uuid')
        _add_info_from_ejudge(run, ejudge_run_id, ejudge_run_uuid, None, judge_id)
        logger.info(f'Run #{run_id} successfully updated')
    except (TypeError, KeyError, ValueError):
        _add_info_from_ejudge(run, None, None, EjudgeStatuses.RMATICS_SUBMIT_ERROR, judge_id)
        ejudge_compiler_output = ejudge_response.get('message', 'Ошибка отправки посылки') if isinstance(ejudge_response, dict) else 'Ошибка отправки посылки'
        run.protocol = _build_submit_error_protocol(run_id, ejudge_compiler_output)
        logger.error(f'Ejudge returned error for submit #{run_id}')
