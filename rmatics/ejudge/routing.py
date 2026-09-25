"""Choosing the judge a run is sent to (judges_settings of the problem)."""
from typing import NamedTuple, Optional

from celery.utils.log import get_task_logger
from werkzeug.exceptions import BadRequest

from rmatics.ejudge.judges_config import get_default_judge_id, get_judge

logger = get_task_logger(__name__)


class LanguageNotSupported(BadRequest):
    """No judge of the problem accepts the language."""
    error_code = 'language_not_supported'
    description = 'Язык не поддерживается для этой задачи'

    def __init__(self, reason: str):
        super().__init__()
        self.reason = reason  # for logs; description is shown to the user


class LanguageNotAllowed(BadRequest):
    """The statement (contest) doesn't allow the language."""
    error_code = 'language_not_allowed'
    description = 'Язык запрещён в этом контесте'


class Route(NamedTuple):
    judge_id: Optional[int]
    contest_id: int
    prob_id: int


_REQUIRED_ENTRY_KEYS = ('judge_id', 'contest_id', 'problem_id')


def _get_judge_entry(problem, lang_id: int, user_id: int) -> Optional[dict]:
    """Return the highest-priority matching judges_settings entry for (lang_id, user_id).

    judges_settings is a list of entries:
      {
        "judge_id":  <int>,    # required — references a judge in judges.json
        "contest_id": <int>,   # required — contest_id inside that ejudge
        "problem_id": <int>,   # required — prob_id inside the contest
        "lang_ids":  [<int>],  # null / absent matches any language
        "user_ids":  [<int>]   # null / absent matches any moodle user
      }

    An entry is a candidate when BOTH filters match:
      - lang_ids is null  OR  lang_id  in lang_ids
      - user_ids is null  OR  user_id  in user_ids

    Entries missing a required key, or with a non-integer judge_id, are
    skipped with a warning.
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
        try:
            int(entry['judge_id'])
        except (TypeError, ValueError):
            logger.warning(
                f'Problem #{problem.id}: judges_settings entry with invalid judge_id, '
                f'skipping: {entry!r}'
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
    LanguageNotSupported is raised instead of falling back to the default
    judge. Problems without judges_settings go to the default judge, and
    are checked against its langs when it declares them.

    judge_id may be None (a problem without judges_settings and no
    DEFAULT_JUDGE_ID) and may be unknown to the config: reporting that is up
    to the caller.
    """
    entry = _get_judge_entry(problem, lang_id, user_id)

    if entry is None:
        if problem.judges_settings:
            raise LanguageNotSupported(
                f'Problem #{problem.id}: no judges_settings entry for lang_id {lang_id}'
            )
        route = Route(get_default_judge_id(), problem.ejudge_contest_id, problem.problem_id)
    else:
        route = Route(int(entry['judge_id']), entry['contest_id'], entry['problem_id'])

    judge = get_judge(route.judge_id)
    # output-only answers are plain text, not a language of the judge
    if judge is not None and not problem.output_only and not judge.supports_lang(lang_id):
        raise LanguageNotSupported(
            f'Problem #{problem.id}: judge {route.judge_id} does not support lang_id {lang_id}'
        )

    return route
