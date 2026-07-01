import requests
from typing import Optional

from flask import current_app

from .xml import parse_xml_testing_report
from .bson import parse_bson_testing_report

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

    if 'xml' in content_type:
        try:
            return parse_xml_testing_report(resp.text, run_id)
        except:
            current_app.logger.exception(
                f'notify: cannot parse testing-report for '
                f'ej_run {ej_run_id}/{ej_contest_id}'
            )
            return None
    elif 'bson' in content_type:
        try:
            return parse_bson_testing_report(resp.content, run_id)
        except:
            current_app.logger.exception(
                f'notify: cannot parse bson testing-report for '
                f'ej_run {ej_run_id}/{ej_contest_id}'
            )
            return None
    else:
        # отчёт недоступен — статус уже обновили,
        # потесты пропускаем.
        current_app.logger.info(
            f'(Content-Type={content_type!r}); protocol skipped'
        )
        current_app.logger.info(
            "ejudge response status=%s content-type=%s body=%s",
            resp.status_code,
            resp.headers.get('Content-Type'),
            resp.text[:500],
        )
        return None