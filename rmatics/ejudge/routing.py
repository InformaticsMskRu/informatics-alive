"""Choosing the judge a run is sent to (judges_settings of the problem)."""
from typing import NamedTuple, Optional

from celery.utils.log import get_task_logger
from werkzeug.exceptions import BadRequest

from rmatics.ejudge.judges_config import get_default_judge_id, get_judge

logger = get_task_logger(__name__)


class LanguageNotSupported(BadRequest):
    """No judge the problem routes to accepts the language."""
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

    An entry is a candidate when:
      - lang_ids is null  OR  lang_id  in lang_ids
      - user_ids is null  OR  user_id  in user_ids
      - its judge has lang_id in its langs; lang_ids can only narrow the
        judge's languages down. Not checked for output-only problems, nor
        for a judge missing from the config (the caller reports that).

    Entries missing a required key, or with a non-integer judge_id, are
    skipped with a warning.
    Among candidates, higher specificity (more filters set) wins; listed
    order breaks ties. Returns None when no entry can take the run.
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
           (user_ids is None or user_id in user_ids) and \
           _judge_supports(problem, entry, lang_id):
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


def _judge_supports(problem, entry: dict, lang_id: int) -> bool:
    # output-only answers are plain text, not a language of the judge
    if problem.output_only:
        return True
    judge = get_judge(int(entry['judge_id']))
    if judge is None or judge.supports_lang(lang_id):
        return True
    if entry.get('lang_ids') is not None:
        logger.warning(
            f'Problem #{problem.id}: lang_ids lists lang_id {lang_id}, which judge '
            f'{entry["judge_id"]} does not support: {entry!r}'
        )
    return False


def resolve_route(problem, lang_id: int, user_id: int) -> Route:
    """Where a run in lang_id by user_id goes.

    A problem with judges_settings accepts a language only if some matching
    entry's judge supports it (see _get_judge_entry); otherwise
    LanguageNotSupported is raised instead of falling back to the default
    judge. Problems without judges_settings go to the default judge and are
    checked against its langs.

    judge_id may be None (a problem without judges_settings and no
    DEFAULT_JUDGE_ID) and may be unknown to the config: reporting that is up
    to the caller.
    """
    if problem.judges_settings:
        entry = _get_judge_entry(problem, lang_id, user_id)
        if entry is None:
            raise LanguageNotSupported(
                f'Problem #{problem.id}: no judges_settings entry routes lang_id {lang_id} '
                f'to a judge that supports it'
            )
        return Route(int(entry['judge_id']), entry['contest_id'], entry['problem_id'])

    route = Route(get_default_judge_id(), problem.ejudge_contest_id, problem.problem_id)
    judge = get_judge(route.judge_id)
    # output-only answers are plain text, not a language of the judge
    if judge is not None and not problem.output_only and not judge.supports_lang(lang_id):
        raise LanguageNotSupported(
            f'Problem #{problem.id}: default judge {route.judge_id} does not support '
            f'lang_id {lang_id}'
        )
    return route
