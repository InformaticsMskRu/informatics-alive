"""Privileged ejudge JSON API calls (judge url is new-master) used to
import problems.

Replies are {"ok": true, ...} on success and
{"ok": false, "error": {"num", "symbol", "message"}} on failure.
"""
import json
from typing import Optional, Tuple

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
    except requests.RequestException as e:
        raise EjudgeApiError(f'{action} failed: {e}') from e
    try:
        data = resp.json()
    except ValueError as e:
        raise EjudgeApiError(f'{action} failed: {_describe_non_json(resp)}') from e

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


_BODY_SNIPPET = 200


def _describe_non_json(resp) -> str:
    # e.g. an ejudge without the action replies with an HTML page
    body = resp.content[:_BODY_SNIPPET].decode('utf-8', 'replace')
    return (f'not a JSON reply: HTTP {resp.status_code}, '
            f'{resp.headers.get("Content-Type") or "no content type"}, '
            f'{len(resp.content)} bytes: {body!r}')


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


def get_file(judge: JudgeConfig, contest_id: int, prob_id: int, name: str) -> Tuple[str, bytes]:
    """A file of the problem's attachments/ directory: (content type, bytes).

    A missing file is not an error status: ejudge replies with an error
    (JSON {"ok": false} to a token), the caller checks the content.
    """
    try:
        resp = requests.get(
            judge.url,
            params={'action': 'get-file', 'contest_id': contest_id,
                    'prob_id': prob_id, 'file': name},
            headers={'Authorization': 'Bearer ' + judge.get_token()},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as e:
        raise EjudgeApiError(f'get-file {name} failed: {e}') from e
    content_type = _media_type(resp.headers.get('Content-Type', ''))
    body = resp.content
    # To a token ejudge replies application/json, and get-file writes the
    # file's own CGI headers into the body: the file follows them
    if body[:len(_CGI_CONTENT_TYPE)].lower() == _CGI_CONTENT_TYPE:
        headers, separator, file_body = body.partition(b'\n\n')
        if separator:
            content_type = _media_type(headers.split(b'\n', 1)[0].split(b':', 1)[1].decode('latin-1'))
            body = file_body
    return content_type, body


_CGI_CONTENT_TYPE = b'content-type:'


def _media_type(content_type: str) -> str:
    return content_type.split(';')[0].strip().lower()


def describe_error(body: bytes) -> Optional[str]:
    """The error of an ejudge JSON error reply, None for another reply."""
    try:
        error = json.loads(body.decode('utf-8'))['error']
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(error, dict):
        return None
    return ' '.join(str(part) for part in (error.get('symbol'), error.get('message')) if part) or None


def get_contest_name(judge: JudgeConfig, contest_id: int) -> str:
    data = _call(judge, contest_id, 'contest-status-json')
    try:
        return data['result']['contest']['name']
    except (KeyError, TypeError):
        raise EjudgeApiError('contest-status-json failed: no contest name in the reply')
