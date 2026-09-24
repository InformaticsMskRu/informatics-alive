"""Choosing the judge a run is sent to (judges_settings of the problem)."""
from typing import NamedTuple, Optional

from celery.utils.log import get_task_logger

from rmatics.ejudge.judges_config import get_default_judge_id, get_judge

logger = get_task_logger(__name__)


LANGUAGE_NOT_AVAILABLE_MESSAGE = 'Язык недоступен для этой задачи'


class LanguageNotAvailable(Exception):
    pass


class Route(NamedTuple):
    judge_id: Optional[int]
    contest_id: int
    prob_id: int


_REQUIRED_ENTRY_KEYS = ('contest_id', 'problem_id')


def _get_judge_entry(problem, lang_id: int, user_id: int) -> Optional[dict]:
    """Return the highest-priority matching judges_settings entry for (lang_id, user_id).

    judges_settings is a list of entries:
      {
        "judge_id":  <str>,    # optional — references a judge in judges.json
        "contest_id": <int>,   # required — contest_id inside that ejudge
        "problem_id": <int>,   # required — prob_id inside the contest
        "lang_ids":  [<int>],  # null / absent matches any language
        "user_ids":  [<int>]   # null / absent matches any moodle user
      }

    An entry is a candidate when BOTH filters match:
      - lang_ids is null  OR  lang_id  in lang_ids
      - user_ids is null  OR  user_id  in user_ids

    judges_settings entry shape:
      {
        "judge_id":  <int>,    # optional — references a judge in judges.json by numeric id
        "contest_id": <int>,   # required
        "problem_id": <int>,   # required
        "lang_ids":  [<int>],  # null / absent matches any language
        "user_ids":  [<int>]   # null / absent matches any moodle user
      }

    Entries missing contest_id or problem_id are skipped with a warning.
    Among valid candidates, higher specificity (more filters set) wins;
    listed order breaks ties. Returns None when no entry matches.
    """
    settings = problem.judges_settings
    if not settings:
        return None

    candidates = []
    for entry in settings:
        missing = [k for k in _REQUIRED_ENTRY_KEYS if k not in entry]
        if missing:
            logger.warning(
                f'Problem #{problem.id}: judges_settings entry missing required keys '
                f'{missing!r}, skipping: {entry!r}'
            )
            continue
        lang_ids = entry.get('lang_ids')
        user_ids = entry.get('user_ids')
        if (lang_ids is None or lang_id in lang_ids) and \
           (user_ids is None or user_id in user_ids):
            candidates.append(entry)

    if not candidates:
        return None

    candidates.sort(
        key=lambda e: -(
            (e.get('lang_ids') is not None) +
            (e.get('user_ids') is not None)
        )
    )
    return candidates[0]


def resolve_route(problem, lang_id: int, user_id: int) -> Route:
    """Where a run in lang_id by user_id goes.

    A problem with judges_settings accepts only languages that some entry
    routes to a judge supporting them (see JudgeConfig.langs); otherwise
    LanguageNotAvailable is raised instead of falling back to the default
    judge. Problems without judges_settings always go to the default judge.

    judge_id may be None (no DEFAULT_JUDGE_ID) and may be unknown to the
    config: reporting that is up to the caller.
    """
    entry = _get_judge_entry(problem, lang_id, user_id)

    if entry is None:
        if problem.judges_settings:
            raise LanguageNotAvailable(
                f'Problem #{problem.id}: no judges_settings entry for lang_id {lang_id}'
            )
        return Route(get_default_judge_id(), problem.ejudge_contest_id, problem.problem_id)

    judge_id = entry.get('judge_id')
    if judge_id is None:
        judge_id = get_default_judge_id()

    judge = get_judge(judge_id)
    # output-only answers are plain text, not a language of the judge
    if judge is not None and not problem.output_only and not judge.supports_lang(lang_id):
        raise LanguageNotAvailable(
            f'Problem #{problem.id}: judge {judge_id} does not support lang_id {lang_id}'
        )

    return Route(judge_id, entry['contest_id'], entry['problem_id'])
