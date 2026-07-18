import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from flask import Flask, current_app

@dataclass
class JudgeConfig:
    url: str
    name: str = field(default='')
    token: Optional[str] = field(default=None)
    sender_user_id: int = field(default=5)
    lang_map: Dict[int, int] = field(default_factory=dict)

    def get_token(self) -> Optional[str]:
        return self.token

    def map_lang_id(self, lang_id: int) -> int:
        return self.lang_map.get(lang_id, lang_id)


def _load(path: str) -> Dict[int, JudgeConfig]:
    with open(path) as f:
        data = json.load(f)
    return {
        int(jid): JudgeConfig(
            url=cfg['url'],
            name=cfg.get('name', ''),
            token=cfg.get('token'),
            sender_user_id=cfg.get('sender_user_id', 5),
            lang_map={int(k): v for k, v in cfg.get('lang_map', {}).items()}
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
    raw = current_app.config.get('DEFAULT_JUDGE_ID')
    return int(raw) if raw is not None else None

def get_judge(judge_id: int) -> Optional[JudgeConfig]:
    return current_app.extensions.get('judges', {}).get(judge_id)
