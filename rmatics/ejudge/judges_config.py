from enum import Enum
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from flask import Flask, current_app

class ConfigMode(Enum):
    OLD = 1
    NEW = 2

@dataclass
class JudgeConfig:
    url: str
    name: str = field(default='')
    token: Optional[str] = field(default=None)
    sender_user_id: int = field(default=5)
    lang_map: Dict[int, int] = field(default_factory=dict)
    mode: int = field(default=ConfigMode.NEW.value)
    queue_name: str = field(default='ejudge.notify')

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
            lang_map={int(k): v for k, v in cfg.get('lang_map', {}).items()},
            mode=cfg.get('mode', ConfigMode.NEW.value),
            queue_name=cfg.get('queue_name', 'ejudge.notify')
        )
        for jid, cfg in data.items()
    }


def init_app(app: Flask) -> None:
    path = app.config.get('JUDGES_CONFIG_PATH')
    if not path:
        app.extensions['judges'] = {}
        app.extensions['queues'] = {}
    else:
        try:
            judges = _load(path)
            queues = dict()
            for jid in judges:
                judge = judges[jid]
                if judge.mode != ConfigMode.NEW.value:
                    continue
                if judge.queue_name in queues:
                    app.logger.warning(f'Queue name "{judge.queue_name}" is used for two judges')
                queues[judge.queue_name] = jid
            app.extensions['judges'] = judges
            app.extensions['queues'] = queues
            app.logger.info(f'Loaded {len(app.extensions["judges"])} judge(s) from {path!r}')
        except Exception:
            app.logger.exception(f'Failed to load judges config from {path!r}')
            app.extensions['judges'] = {}
            app.extensions['queues'] = {}

    default_id = app.config.get('DEFAULT_JUDGE_ID')
    if not default_id:
        app.logger.warning(
            'DEFAULT_JUDGE_ID not set: runs using the default judge path will not '
            'record judge_id, preventing protocol archiving on rejudge'
        )
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

def get_jid_by_queue(queue_name: str) -> Optional[int]:
    return current_app.extensions.get('queues', {}).get(queue_name)

def get_all_streams() -> List[str]:
    return list(current_app.extensions.get('queues', {}).keys())