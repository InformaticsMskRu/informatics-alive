from celery import shared_task
from celery.utils.log import get_task_logger

from werkzeug.exceptions import BadRequest
from sqlalchemy import and_, update
from typing import Optional

from rmatics.ejudge.judges_config import get_judge
from rmatics.model.base import db
from rmatics.model.run import Run
from rmatics.utils.cacher.helpers import invalidate_monitor_cache_by_run

from rmatics.ejudge.protocol import fetch_protocol

from rmatics.utils.run import EjudgeStatuses

logger = get_task_logger(__name__)

NON_TERMINAL_STATUSES = {
    EjudgeStatuses.COMPILING.value,  # 98
    EjudgeStatuses.RUNNING.value,    # 96
    EjudgeStatuses.IN_QUEUE.value,   # 377
}

def _is_terminal(status: Optional[int]) -> bool:
    return status is not None and status not in NON_TERMINAL_STATUSES

def _resolve_judge(judge_id: int):
    """(url, token) ejudge'а, у которого спрашивать протокол."""
    judge = get_judge(judge_id)
    return judge.url, judge.get_token()

def _get_run(data) -> Run:
    try:
        run_id = data.get('rmatics_run_id')
        if run_id is not None:
            run = db.session.query(Run) \
                .filter_by(id=int(run_id)) \
                .one_or_none()
        else:
            run = db.session.query(Run) \
                .filter_by(ejudge_run_uuid=data["run_uuid"],
                       judge_id=data["judge_id"]) \
                .one_or_none()
    except:
        msg = f'Cannot find Run with run_id={data.get("rmatics_run_id")}, judge_id={data["judge_id"]}, ejudge_run_uuid={data["run_uuid"]}.'
        logger.exception(msg)
        raise BadRequest(msg)
    
    if run is None:
        msg = f'Cannot find Run with run_id={data.get("rmatics_run_id")}, judge_id={data["judge_id"]}, ejudge_run_uuid={data["run_uuid"]}.'
        logger.exception(msg)
        raise BadRequest(msg)

    return run

def _to_int(value) -> Optional[int]:
    try:
        return int(value)
    except:
        return None

@shared_task(name='rmatics.tasks.notify.check_run', bind=True, max_retries=None, retry_backoff=True)
def check_run(self, data) -> dict:
    try:
        _get_run(data)
    except Exception as e:
        logger.info('retry run')
        self.retry(exc=e)

    return data

@shared_task(name='rmatics.tasks.notify.load_protocol', bind=True, default_retry_delay=30, max_retries=5)
def load_protocol(self, data) -> dict:
    run = _get_run(data)

    if _is_terminal(data["status"]):
        url, token = _resolve_judge(int(data["judge_id"]))
        try:
            protocol = fetch_protocol(url, token, data["contest_id"], data["run_id"], run.id)
            if protocol is not None:
                run.protocol = protocol
        except Exception as exc:
            if self.request.retries < load_protocol.max_retries:
                logger.info('retry protocol')
                self.retry(exc=exc)
            logger.warning('Failed to load protocol Max retries count exceed. Aborting.'
                          f'Request args={data}')
            raise exc
        
    return data

@shared_task(name='rmatics.tasks.notify.upd_run', ignore_result=True, retry=False)
def upd_run(data):
    ejudge_run_id = _to_int(data.get('run_id'))
    ejudge_run_uuid = data.get('run_uuid')
    ejudge_contest_id = _to_int(data.get('contest_id'))
    status = _to_int(data.get('status'))
    judge_id = _to_int(data.get('judge_id'))

    values = {}
    if status is not None:
        values[Run.ejudge_status] = status

    score = _to_int(data.get('score'))
    if score is not None:
        values[Run.ejudge_score] = score

    test = _to_int(data.get('test_num'))
    if test is not None:
        values[Run.ejudge_test_num] = test

    values[Run.ejudge_run_id] = ejudge_run_id
    values[Run.ejudge_run_uuid] = ejudge_run_uuid
    values[Run.ejudge_contest_id] = ejudge_contest_id

    rmatics_run_id = data.get('rmatics_run_id')
    if rmatics_run_id is not None:
        where = and_(Run.id == rmatics_run_id,
                        Run.ejudge_status.in_(NON_TERMINAL_STATUSES))
    else:
        where = and_(Run.ejudge_run_uuid == ejudge_run_uuid,
                     Run.judge_id == judge_id,
                        Run.ejudge_status.in_(NON_TERMINAL_STATUSES))

    applied = db.session.execute(update(Run).where(where).values(values)).rowcount
    db.session.commit()

    if applied > 0:
        logger.info(f'Run was updated successfully')
    else:
        logger.info(f'Skipping update: already terminal')
    return data

@shared_task(name='rmatics.tasks.notify.invalidate_cache', bind=True, ignore_result=True, default_retry_delay=5, max_retries=3)
def invalidate_cache(self, data):
    try:
        run = _get_run(data)
    except Exception as e:
        logger.info('retry invalidate')
        self.retry(exc=e)

    invalidate_monitor_cache_by_run(run)

def make_terminal_upd_chain():
    upd_chain = check_run.s() | load_protocol.s() | upd_run.s() | invalidate_cache.s()
    return upd_chain

def make_nonterminal_upd_chain():
    upd_chain = check_run.s() | upd_run.s() | invalidate_cache.s()
    return upd_chain
