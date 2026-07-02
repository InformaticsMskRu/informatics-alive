from celery import shared_task
from celery.utils.log import get_task_logger

from typing import Optional
from rmatics.model import Run
from rmatics.model.base import db
from sqlalchemy.orm import joinedload
from rmatics.utils.run import EjudgeStatuses

from rmatics.ejudge.judges_config import get_judge, get_default_judge_id
from rmatics.ejudge.ejudge_proxy import submit

from rmatics import centrifugo_client

logger = get_task_logger(__name__)

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
            logger.warning(
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

@shared_task(name='rmatics.ejudge.submit_queue.task.submit_task', ignore_result=True, retry=False)
def submit_task(run_id):
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

    entry = _get_judge_entry(problem, run.lang_id, run.user_id)

    if entry is not None:
        judge_id = entry.get('judge_id')
        contest_id = entry['contest_id']
        prob_id = entry['problem_id']
    else:
        judge_id = get_default_judge_id()
        contest_id = problem.ejudge_contest_id
        prob_id = problem.problem_id

    if judge_id is None:
        judge_id = get_default_judge_id()

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
    except Exception:
        logger.exception(
            f'Run #{run_id}: submit to judge {judge_id!r} raised exception'
        )
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
        ejudge_compiler_output = ejudge_response.get('message', 'Ошибка отправки посылки')
        run.protocol = _build_submit_error_protocol(ejudge_compiler_output)
        logger.error(f'Ejudge returned error for submit #{run_id}')
