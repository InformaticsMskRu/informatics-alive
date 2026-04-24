import json
from dataclasses import dataclass, field
from typing import Dict, Optional

from flask import Flask, current_app


@dataclass
class JudgeConfig:
    url: str
    token: Optional[str] = field(default=None)
    sender_user_id: int = field(default=5)
    lang_map: Dict[int, int] = field(default_factory=dict)

    def get_token(self) -> str:
        return self.token or current_app.config.get('EJUDGE_MASTER_TOKEN')

    def map_lang_id(self, lang_id: int) -> int:
        return self.lang_map.get(lang_id, lang_id)


def _load(path: str) -> Dict[str, JudgeConfig]:
    with open(path) as f:
        data = json.load(f)
    return {
        jid: JudgeConfig(
            url=cfg['url'],
            token=cfg.get('token'),
            sender_user_id=cfg.get('sender_user_id', 5),
            lang_map={int(k): v for k, v in cfg.get('lang_map', {}).items()},
        )
        for jid, cfg in data.items()
    }


def init_app(app: Flask) -> None:
    path = app.config.get('JUDGES_CONFIG_PATH')
    if not path:
        app.extensions['judges'] = {}
    else:
        try:
            app.extensions['judges'] = _load(path)
            app.logger.info(f'Loaded {len(app.extensions["judges"])} judge(s) from {path!r}')
        except Exception:
            app.logger.exception(f'Failed to load judges config from {path!r}')
            app.extensions['judges'] = {}

    default_id = app.config.get('DEFAULT_JUDGE_ID')
    if not default_id:
        app.logger.warning(
            'DEFAULT_JUDGE_ID not set: runs using the default judge path will not '
            'record judge_id, preventing protocol archiving on rejudge'
        )
    elif default_id not in app.extensions['judges']:
        app.logger.warning(
            f'DEFAULT_JUDGE_ID {default_id!r} not found in loaded judges config'
        )


def get_judge(judge_id: str) -> Optional[JudgeConfig]:
    return current_app.extensions.get('judges', {}).get(judge_id)
