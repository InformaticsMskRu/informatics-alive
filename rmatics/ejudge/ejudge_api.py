"""Privileged ejudge JSON API calls (judge url is new-master) used to
import problems.

Replies are {"ok": true, ...} on success and
{"ok": false, "error": {"num", "symbol", "message"}} on failure.
"""
import requests

from rmatics.ejudge.judges_config import JudgeConfig

REQUEST_TIMEOUT = 10  # seconds


class EjudgeApiError(Exception):
    """ejudge is unreachable, rejected the request or replied unexpectedly."""


class EjudgeNotFound(EjudgeApiError):
    """ejudge has no such contest or problem."""


def _call(judge: JudgeConfig, contest_id: int, action: str, **params) -> dict:
    try:
        resp = requests.get(
            judge.url,
            params={'action': action, 'contest_id': contest_id, **params},
            headers={'Authorization': 'Bearer ' + judge.get_token()},
            timeout=REQUEST_TIMEOUT,
        )
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        raise EjudgeApiError(f'{action} failed: {e}') from e

    if not isinstance(data, dict):
        raise EjudgeApiError(f'{action} failed: unexpected reply')
    if not data.get('ok'):
        error = data.get('error') or {}
        message = f'{action} failed: {error.get("symbol") or resp.status_code}'
        if error.get('message'):
            message += f' ({error["message"]})'
        if resp.status_code == 404:
            raise EjudgeNotFound(message)
        raise EjudgeApiError(message)
    return data


def get_problem(judge: JudgeConfig, contest_id: int, prob_id: int) -> dict:
    """The fully resolved problem section (abstract problem fields inherited).

    Sizes come in bytes (size_mode=1); unset fields are left out.
    """
    # without abstract=0 ejudge doesn't look a concrete problem up by prob_id
    data = _call(judge, contest_id, 'get-problem-json',
                 prob_id=prob_id, abstract=0, size_mode=1)
    problem = data.get('problem')
    if not isinstance(problem, dict):
        raise EjudgeApiError('get-problem-json failed: no problem in the reply')
    return problem


def list_problems(judge: JudgeConfig, contest_id: int) -> list:
    """The concrete problems of the contest, resolved like get_problem."""
    data = _call(judge, contest_id, 'list-problems-json', size_mode=1)
    problems = data.get('problems')
    if not isinstance(problems, list):
        raise EjudgeApiError('list-problems-json failed: no problems in the reply')
    return problems


def get_contest_name(judge: JudgeConfig, contest_id: int) -> str:
    data = _call(judge, contest_id, 'contest-status-json')
    try:
        return data['result']['contest']['name']
    except (KeyError, TypeError):
        raise EjudgeApiError('contest-status-json failed: no contest name in the reply')
