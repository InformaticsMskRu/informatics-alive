import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from flask import Flask, current_app

logger = logging.getLogger(__name__)

@dataclass
class JudgeLang:
    name: str            # short name with version, e.g. 'Python 3.9'
    ejudge_lang_id: int  # id of this language inside the judge's ejudge


@dataclass
class JudgeConfig:
    url: str
    name: str = field(default='')
    token: Optional[str] = field(default=None)
    sender_user_id: int = field(default=5)
    # rmatics lang_id -> language of the judge. None: the judge doesn't
    # declare its languages (no language check, lang_ids sent as is).
    langs: Optional[Dict[int, JudgeLang]] = field(default=None)

    def get_token(self) -> Optional[str]:
        return self.token

    def supports_lang(self, lang_id: int) -> bool:
        return self.langs is None or lang_id in self.langs

    def map_lang_id(self, lang_id: int) -> int:
        if self.langs is not None:
            if lang_id not in self.langs:
                # routing (resolve_route) only lets supported languages through
                raise ValueError(f'lang_id {lang_id} is not in the judge langs')
            return self.langs[lang_id].ejudge_lang_id
        return lang_id


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_langs(jid, raw) -> Optional[Dict[int, JudgeLang]]:
    """{"<lang_id>": {"name": <str>, "ejudge_lang_id": <int>}}; invalid
    entries are skipped with a warning."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        logger.warning(f'Judge {jid}: "langs" must be an object, ignoring it')
        return None

    langs = {}
    for lang_id, lang in raw.items():
        try:
            lang_id = int(lang_id)
        except (TypeError, ValueError):
            logger.warning(f'Judge {jid}: invalid lang_id {lang_id!r} in "langs", skipping')
            continue
        name = lang.get('name') if isinstance(lang, dict) else None
        ejudge_lang_id = lang.get('ejudge_lang_id') if isinstance(lang, dict) else None
        if not isinstance(name, str) or not name.strip() or not _is_int(ejudge_lang_id):
            logger.warning(
                f'Judge {jid}: lang_id {lang_id} in "langs" needs "name" and an integer '
                f'"ejudge_lang_id", skipping: {lang!r}'
            )
            continue
        langs[lang_id] = JudgeLang(name=name.strip(), ejudge_lang_id=ejudge_lang_id)
    return langs


def _load(path: str) -> Dict[int, JudgeConfig]:
    with open(path) as f:
        data = json.load(f)
    for jid, cfg in data.items():
        if 'lang_map' in cfg:
            # its mapping is not applied: languages would reach ejudge with wrong ids
            logger.warning(f'Judge {jid}: "lang_map" is not supported, move it into "langs"')
    return {
        int(jid): JudgeConfig(
            url=cfg['url'],
            name=cfg.get('name', ''),
            token=cfg.get('token'),
            sender_user_id=cfg.get('sender_user_id', 5),
            langs=_parse_langs(jid, cfg.get('langs')),
        )
        for jid, cfg in data.items()
    }

def _validate(app: Flask, judges: Dict[int, JudgeConfig]) -> bool:
    for jid in judges:
        judge = judges[jid]
        if judge.token is None:
            app.logger.error(f'No token provided for judge {jid}')
            return False
    
    return True

def init_app(app: Flask) -> None:
    path = app.config.get('JUDGES_CONFIG_PATH')
    if not path:
        app.extensions['judges'] = {}
    else:
        try:
            judges = _load(path)

            if _validate(app, judges):
                app.extensions['judges'] = judges
                app.logger.info(f'Loaded {len(app.extensions["judges"])} judge(s) from {path!r}')
            else:
                app.extensions['judges'] = {}

        except Exception:
            app.logger.exception(f'Failed to load judges config from {path!r}')
            app.extensions['judges'] = {}

    default_id = app.config.get('DEFAULT_JUDGE_ID')
    if not default_id:
        app.logger.error('DEFAULT_JUDGE_ID not set')
    else:
        try:
            default_id_int = int(default_id)
        except (TypeError, ValueError):
            app.logger.warning(f'DEFAULT_JUDGE_ID {default_id!r} is not a valid integer')
            default_id_int = None
        if default_id_int is not None and default_id_int not in app.extensions['judges']:
            app.logger.warning(
                f'DEFAULT_JUDGE_ID {default_id!r} not found in loaded judges config'
            )


def get_default_judge_id() -> Optional[int]:
    # an invalid value is already reported by init_app
    try:
        return int(current_app.config.get('DEFAULT_JUDGE_ID'))
    except (TypeError, ValueError):
        return None

def get_judge(judge_id: int) -> Optional[JudgeConfig]:
    return current_app.extensions.get('judges', {}).get(judge_id)
