import logging
import time
from typing import List

logger = logging.getLogger(__name__)


class ImportLog:
    """The steps of a reload: written to the server log and returned in the
    reply, so whoever reloads sees how it went."""

    def __init__(self, judge_id: int, contest_id: int):
        self._prefix = f'Judge {judge_id} contest {contest_id}'
        self._start = time.monotonic()
        self.lines: List[str] = []

    def info(self, message: str):
        self._add(logging.INFO, message)

    def warning(self, message: str):
        self._add(logging.WARNING, message)

    def _add(self, level: int, message: str):
        logger.log(level, f'{self._prefix}: {message}')
        elapsed = time.monotonic() - self._start
        self.lines.append(f'{elapsed:.2f}s {logging.getLevelName(level).lower()}: {message}')
