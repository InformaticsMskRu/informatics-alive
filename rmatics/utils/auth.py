"""Авторизация внешних клиентов rmatics по Bearer-токену.
* TRUSTED_TOKEN - под которым pynformatics ходит в trusted api;
* ejudge api token - под ним ejudge-listener и notify-worker присылают нотификации 
о посылках в update_from_ejudge / update_from_ejudge_v2.
"""
import functools
from hmac import compare_digest
from typing import Optional

from flask import current_app, request
from werkzeug.exceptions import Forbidden, Unauthorized

from rmatics.ejudge.judges_config import get_judge


def _get_request_token() -> str:
    header = request.headers.get('Authorization', '')
    scheme, _, token = header.partition(' ')
    token = token.strip()
    if scheme.lower() != 'bearer' or not token:
        raise Unauthorized('Bearer token is required')
    return token


def _matches(token: str, expected: Optional[str]) -> bool:
    if not expected:
        return False
    return compare_digest(token.encode('utf-8'), expected.encode('utf-8'))


def require_trusted_token(view):
    @functools.wraps(view)
    def wrapper(*args, **kwargs):
        token = _get_request_token()
        expected = current_app.config.get('TRUSTED_TOKEN')
        if not expected:
            current_app.logger.error('TRUSTED_TOKEN is not configured')
            raise Forbidden('Invalid token')
        if not _matches(token, expected):
            raise Forbidden('Invalid token')
        return view(*args, **kwargs)
    return wrapper


def require_judge_token(view):
    """
    Старый listener может judge_id не прислать, тогда принимаем токен
    любого известного judge.
    """
    @functools.wraps(view)
    def wrapper(*args, **kwargs):
        token = _get_request_token()
        data = request.get_json(force=True, silent=True) or {}

        try:
            judge_id = int(data.get('judge_id'))
        except (TypeError, ValueError):
            judge_id = None

        if judge_id is not None:
            judge = get_judge(judge_id)
            matched = judge is not None and _matches(token, judge.get_token())
        else:
            current_app.logger.warning(
                'Notification without judge_id, checking token against all judges'
            )
            matched = any(_matches(token, judge.get_token())
                          for judge in current_app.extensions.get('judges', {}).values())

        if not matched:
            raise Forbidden('Invalid token')
        return view(*args, **kwargs)
    return wrapper
