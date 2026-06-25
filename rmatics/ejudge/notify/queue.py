import datetime
import pickle

from rmatics.utils.redis.queue import RedisStreamsQueue
from flask import current_app

import json
import xml.etree.ElementTree as ET
from typing import Optional

import requests
from sqlalchemy import and_, update

from rmatics import centrifugo_client
from rmatics.ejudge.judges_config import get_judge, get_default_judge_id
from rmatics.model.base import db
from rmatics.model.run import Run
from rmatics.utils.cacher.helpers import invalidate_monitor_cache_by_run
from rmatics.utils.run import EjudgeStatuses, get_string_status


NON_TERMINAL_STATUSES = {
    EjudgeStatuses.COMPILING.value,  # 98
    EjudgeStatuses.RUNNING.value,    # 96
    EjudgeStatuses.IN_QUEUE.value,   # 377
}

def _to_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _text(elem) -> str:
    return (elem.text or '') if elem is not None else ''


def _too_big(elem) -> bool:
    return elem is not None and elem.get('too-big') in ('1', 'yes', 'true')

def _is_terminal(status: Optional[int]) -> bool:
    return status is not None and status not in NON_TERMINAL_STATUSES

def _resolve_judge(judge_id: Optional[int]):
    """(url, token) ejudge'а, у которого спрашивать протокол."""
    judge = get_judge(judge_id) if judge_id is not None else None

    url = (
        judge.url if judge else None
    ) or current_app.config['EJUDGE_NEW_MASTER_URL']

    token = (
        judge.get_token() if judge else None
    ) or current_app.config.get('EJUDGE_MASTER_TOKEN')

    return url, token


def _rmatics_run_id(run_data: dict) -> Optional[int]:
    if run_data.get('ext_user_kind') != 'u64':
        return None
    return _to_int(run_data.get('ext_user'))



def parse_testing_report(xml_text: str, run_id: int) -> dict:
    root = ET.fromstring(xml_text)

    tests = {}

    tests_elem = root.find('tests')
    if tests_elem is not None:
        for test in tests_elem.findall('test'):
            num = test.get('num')
            if num is None:
                continue

            inp = test.find('input')
            out = test.find('output')
            corr = test.find('correct')
            checker = test.find('checker')
            err = test.find('stderr')

            status_code = test.get('status', 'UNKOWN')

            try:
                string_status = get_string_status(status_code)
            except KeyError:
                string_status = status_code

            tests[num] = {
                'input': _text(inp),
                'big_input': _too_big(inp),
                'corr': _text(corr),
                'big_corr': _too_big(corr),
                'output': _text(out),
                'big_output': _too_big(out),
                'checker_output': _text(checker),
                'error_output': _text(err),
                'extra': '',
                'status': status_code,
                'string_status': string_status,
                'time': _to_int(test.get('time'), 0),
                'real_time': _to_int(test.get('real-time'), 0),
                'max_memory_used': _to_int(
                    test.get('max-memory-used'),
                    0,
                ),
            }

    return {
        'run_id': run_id,
        'compiler_output': _text(root.find('compiler_output')),
        'tests': tests,
    }


def fetch_protocol(
    url,
    token,
    ej_contest_id,
    ej_run_id,
    run_id,
) -> Optional[dict]:
    """Запросить у ejudge потестовый отчёт по API и распарсить его."""
    headers = (
        {'Authorization': 'Bearer ' + token}
        if token
        else {}
    )

    params = {
        'action': 'raw-report',
        'contest_id': ej_contest_id,
        'run_id': ej_run_id,
        'format': 'xml',
    }

    try:
        resp = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()

    except requests.RequestException:
        current_app.logger.exception(
            f'notify: cannot fetch report for '
            f'ej_run {ej_run_id}/{ej_contest_id}'
        )
        return None

    content_type = resp.headers.get('Content-Type', '')

    if 'xml' not in content_type:
        # BSON-хранилище / отчёт недоступен — статус уже обновили,
        # потесты пропускаем.
        current_app.logger.info(
            f'notify: report for ej_run '
            f'{ej_run_id}/{ej_contest_id} is not XML '
            f'(Content-Type={content_type!r}); protocol skipped'
        )
        current_app.logger.info(
            "ejudge response status=%s content-type=%s body=%s",
            resp.status_code,
            resp.headers.get('Content-Type'),
            resp.text[:500],
        )
        return None

    try:
        return parse_testing_report(resp.text, run_id)

    except ET.ParseError:
        current_app.logger.exception(
            f'notify: cannot parse testing-report for '
            f'ej_run {ej_run_id}/{ej_contest_id}'
        )
        return None


def handle_run_message(timestamp, run_data: dict):
    ej_run_id = _to_int(run_data.get('run_id'))
    ej_contest_id = _to_int(run_data.get('contest_id'))

    if ej_run_id is None or ej_contest_id is None:
        current_app.logger.warning(
            f'notify: message without run_id/contest_id: {run_data!r}'
        )
        return

    status = _to_int(run_data.get('status'))

    values = {}
    if status is not None:
        values[Run.ejudge_status] = status

    score = _to_int(run_data.get('raw_score'))
    if score is not None:
        values[Run.ejudge_score] = score

    test = _to_int(run_data.get('raw_test'))
    if test is not None:
        values[Run.ejudge_test_num] = test

    values[Run.ejudge_run_id] = ej_run_id
    values[Run.ejudge_last_timestamp] = timestamp

    rmatics_run_id = _rmatics_run_id(run_data)

    where = and_(Run.id == rmatics_run_id,
                     Run.ejudge_last_timestamp < timestamp)

    applied = db.session.execute(update(Run).where(where).values(values)).rowcount
    db.session.commit()

    run = db.session.query(Run).get(rmatics_run_id)

    if run is None:
        current_app.logger.warning(
            f'notify: Run not found (ej_run={ej_run_id} ej_contest={ej_contest_id} '
            f'rmatics_run_id={rmatics_run_id})'
        )
        return
    
    run_id, problem_id, run_judge_id = run.id, run.problem_id, run.judge_id
    
    status = _to_int(run_data.get('status'))

    if applied == 0:
        current_app.logger.info(
            f'notify: skip stale status {status} for run #{run_id} (already terminal)'
        )
        return

    invalidate_monitor_cache_by_run(run)

    if _is_terminal(status):
        url, token = _resolve_judge(run_judge_id if run_judge_id is not None else get_default_judge_id())
        protocol = fetch_protocol(url, token, ej_contest_id, ej_run_id, run_id)
        if protocol is not None:
            run.protocol = protocol

    centrifugo_client.send_problem_run_updates(problem_id, run)
    current_app.logger.info(f'notify: Run #{run_id} -> status {status}')


def process_message(raw: str):
    """Разобрать одно сообщение из stream и применить его."""
    try:
        message = json.loads(raw)
    except (TypeError, ValueError):
        current_app.logger.warning(f'notify: cannot decode message {raw!r}')
        return

    msg_type = message.get('type')

    timestamp = _to_int(message.get('server_time_us'))
    
    if msg_type == 'run':
        handle_run_message(timestamp, message.get('run') or {})
    else:
        current_app.logger.debug(f'notify: skip message type {msg_type!r}')

class NotifyQueue(RedisStreamsQueue):
    
    def __init__(self, stream, group, consumer):
        super(NotifyQueue, self).__init__(stream=stream, group=group, consumer=consumer)

    def get_and_process(self):
        resp = super(NotifyQueue, self).get_blocking()
        current_app.logger.info('ejudge notification')
        if not resp:
            return
        for _stream, messages in resp:
            for message_id, fields in messages:
                data = fields.get(b'data') or fields.get('data')

                if isinstance(data, bytes):
                    data = data.decode('utf-8', 'replace')

                try:
                    if data is not None:
                        process_message(data)
                except Exception:
                    current_app.logger.exception(
                        'notify-worker: failed to process message'
                    )
                    db.session.rollback()
                finally:
                    super(NotifyQueue, self).ack(message_id)
