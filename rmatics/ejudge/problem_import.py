"""Import a problem from a judge through the ejudge API: create or update
the informatics problem and route it to that judge with judges_settings.

Imported are the name, limits and type, and the Polygon statement where
ejudge has one (see rmatics.ejudge.statement). Without it a new problem
gets a placeholder, an existing one keeps its own statement.
"""
import base64
import datetime
import logging
from collections import defaultdict
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

from sqlalchemy import Text, text, type_coerce

from rmatics.ejudge import ejudge_api
from rmatics.ejudge.judges_config import JudgeConfig, get_default_judge_id
from rmatics.ejudge.statement import Statement, fetch_statement
from rmatics.model.base import db
from rmatics.model.ejudge_contest import EjudgeContest
from rmatics.model.problem import EjudgeProblem, Problem
from rmatics.utils.moodle import get_contest_str_id

logger = logging.getLogger(__name__)

NO_STATEMENT = '<p>Условие пока не опубликовано...</p>'
# a problem must not have an empty name
NO_NAME = 'Без названия'

# (judge_id, contest_id, prob_id) of an ejudge problem
ProblemKey = Tuple[int, int, int]

IMPORT_LOCK_TIMEOUT = 20  # seconds


class ImportLocked(Exception):
    """Another reload of the contest didn't finish in IMPORT_LOCK_TIMEOUT."""


@contextmanager
def _import_lock(judge_id: int, contest_id: int):
    """Serializes the reloads of a contest on a judge: the imported problems
    are looked up and created under it, so concurrent reloads can't both
    create a problem.

    A MySQL named lock belongs to a connection, while the import commits
    and so returns the session's connection to the pool: the lock is held
    on a connection of its own.
    """
    name = f'rmatics:ejudge_import:{judge_id}:{contest_id}'
    with db.engine.connect() as conn:
        acquired = conn.execute(text('SELECT GET_LOCK(:name, :timeout)'),
                                name=name, timeout=IMPORT_LOCK_TIMEOUT).scalar()
        if acquired != 1:
            raise ImportLocked(f'Contest {contest_id} of judge {judge_id} '
                               f'is being reloaded, try again later')
        try:
            yield
        finally:
            conn.execute(text('SELECT RELEASE_LOCK(:name)'), name=name)


def problem_fields(ejudge_problem: dict) -> dict:
    """Problem values from the ejudge problem, derived the way the
    filesystem reload derives them from serve.cfg."""
    long_name = ejudge_problem.get('long_name') or ''
    if 'time_limit_millis' in ejudge_problem:
        timelimit = ejudge_problem['time_limit_millis'] / 1000
    else:
        timelimit = ejudge_problem.get('time_limit', -1)
    fields = {
        'name': long_name or NO_NAME,
        'ejudge_name': long_name,
        'timelimit': timelimit,
        'memorylimit': ejudge_problem.get('max_vm_size'),
        'output_only': ejudge_problem.get('type') == 'output-only',
    }
    # ejudge doesn't serialize fixed-size string fields and short_name is
    # one (char[32]): without it an existing short id is kept, a new
    # problem gets none
    if ejudge_problem.get('short_name'):
        fields['short_id'] = ejudge_problem['short_name']
    return fields


def _entry_key(entry) -> Optional[ProblemKey]:
    if not isinstance(entry, dict):
        return None
    try:
        return int(entry['judge_id']), int(entry['contest_id']), int(entry['problem_id'])
    except (KeyError, TypeError, ValueError):
        return None


def index_imported_problems(contest_id: int) -> Dict[ProblemKey, List[EjudgeProblem]]:
    """Back-index from the ejudge problems of contest_id (on any judge) to
    the problems already imported from them.

    A problem is imported from an ejudge problem when a judges_settings
    entry routes to it. Problems of the default judge loaded by the
    filesystem reload have no judges_settings and are indexed by the legacy
    ejudge_contest_id/problem_id instead.
    """
    index = defaultdict(list)

    # MariaDB 5.5 has no JSON functions: the contest id digits pick the
    # candidate rows, their entries are compared after parsing
    settings_text = type_coerce(EjudgeProblem.judges_settings, Text)
    candidates = db.session.query(EjudgeProblem) \
        .filter(settings_text.like(f'%{contest_id}%')) \
        .order_by(EjudgeProblem.id) \
        .all()
    for problem in candidates:
        if not isinstance(problem.judges_settings, list):
            continue
        # several entries (e.g. per language) may route to the same problem
        keys = {_entry_key(entry) for entry in problem.judges_settings}
        for key in keys:
            if key is not None and key[1] == contest_id:
                index[key].append(problem)

    default_judge_id = get_default_judge_id()
    if default_judge_id is not None:
        legacy = db.session.query(EjudgeProblem) \
            .filter(EjudgeProblem.ejudge_contest_id == contest_id) \
            .order_by(EjudgeProblem.id) \
            .all()
        for problem in legacy:
            # a problem with judges_settings is routed by them, even if its
            # legacy columns still name an ejudge problem
            if not problem.judges_settings:
                index[(default_judge_id, contest_id, problem.problem_id)].append(problem)
    return index


def _insert_row(table, values: dict) -> int:
    """Insert a row and return its id.

    The ids of mdl_ejudge_problem and mdl_ejudge_contest are AUTO_INCREMENT
    in moodle, but the composite primary keys of their models hide that from
    SQLAlchemy: the id is taken from the cursor.
    """
    return db.session.execute(table.insert().values(**values)).lastrowid


def _get_or_create_contest_id(judge: JudgeConfig, contest_id: int) -> int:
    contest = db.session.query(EjudgeContest) \
        .filter(EjudgeContest.ejudge_int_id == contest_id) \
        .first()
    if contest is not None:
        return contest.id
    return _insert_row(EjudgeContest.__table__, {
        'name': ejudge_api.get_contest_name(judge, contest_id),
        'ejudge_id': get_contest_str_id(contest_id),
        'ejudge_int_id': contest_id,
        'load_time': datetime.datetime.now(),
        'cloned': False,
    })


def _create_problem(judge: JudgeConfig, judge_id: int, contest_id: int, prob_id: int,
                    entry: dict, fields: dict) -> Problem:
    if judge_id == get_default_judge_id():
        legacy = {
            'contest_id': _get_or_create_contest_id(judge, contest_id),
            'ejudge_contest_id': contest_id,
            'problem_id': prob_id,
        }
    else:
        # the legacy columns address the default judge (and its files):
        # a problem of another judge is reached through judges_settings only
        legacy = {'contest_id': 0, 'ejudge_contest_id': 0, 'problem_id': 0}

    ejudge_prid = _insert_row(EjudgeProblem.__table__, {
        **legacy,
        'short_id': fields.get('short_id'),
        'name': fields['ejudge_name'],
        'judges_settings': [entry],
    })
    problem = Problem(
        name=fields['name'],
        timelimit=fields['timelimit'],
        memorylimit=fields['memorylimit'],
        output_only=fields['output_only'],
        content=NO_STATEMENT,
        review='',
        description='',
        analysis='',
        sample_tests='',
        sample_tests_html='',
        pr_id=ejudge_prid,
    )
    db.session.add(problem)
    db.session.flush([problem])
    return problem


def import_problem(judge: JudgeConfig, judge_id: int, contest_id: int, prob_id: int) -> dict:
    """Create the problem, or update the problems already imported from it
    (see index_imported_problems); an imported problem without
    judges_settings gets the entry routing to this ejudge problem."""
    ejudge_problem = ejudge_api.get_problem(judge, contest_id, prob_id)
    statement = fetch_statement(judge, contest_id, prob_id)
    with _import_lock(judge_id, contest_id):
        index = index_imported_problems(contest_id)
        return _import(judge, judge_id, contest_id, prob_id, ejudge_problem, statement, index)


def import_contest(judge: JudgeConfig, judge_id: int, contest_id: int) -> dict:
    """import_problem for every problem of the contest.

    Each problem is committed on its own: a failure leaves the problems
    before it imported.
    """
    ejudge_problems = ejudge_api.list_problems(judge, contest_id)
    for ejudge_problem in ejudge_problems:
        if not isinstance(ejudge_problem.get('id'), int):
            raise ejudge_api.EjudgeApiError(
                f'list-problems-json failed: problem without id: {ejudge_problem!r}')
    logger.info(f'Judge {judge_id} contest {contest_id}: {len(ejudge_problems)} problem(s) to reload')
    # the ejudge requests are made before the lock is taken
    statements = [fetch_statement(judge, contest_id, ejudge_problem['id'])
                  for ejudge_problem in ejudge_problems]

    results = []
    with _import_lock(judge_id, contest_id):
        index = index_imported_problems(contest_id)
        for ejudge_problem, statement in zip(ejudge_problems, statements):
            results.append(_import(judge, judge_id, contest_id, ejudge_problem['id'],
                                   ejudge_problem, statement, index))
    return {'problems': results}


def _import(judge: JudgeConfig, judge_id: int, contest_id: int, prob_id: int,
            ejudge_problem: dict, statement: Optional[Statement],
            index: Dict[ProblemKey, List[EjudgeProblem]]) -> dict:
    fields = problem_fields(ejudge_problem)
    entry = {'judge_id': judge_id, 'contest_id': contest_id, 'problem_id': prob_id}

    problems = index.get((judge_id, contest_id, prob_id))
    # TODO: a problem routed to several judges (entries of different judges)
    # gets its name and limits from whichever of them was reloaded last;
    # decide which judge such a problem is imported from
    if problems:
        action = 'update'
        for problem in problems:
            for key, value in fields.items():
                setattr(problem, key, value)
            if not problem.judges_settings:
                problem.judges_settings = [entry]
    else:
        action = 'create'
        problems = [_create_problem(judge, judge_id, contest_id, prob_id, entry, fields)]
    if statement is not None:
        for problem in problems:
            problem.content = statement.content_for(problem.id)
            problem.sample_tests_html = statement.sample_tests_html
    db.session.commit()
    logger.info(f'Judge {judge_id} contest {contest_id} problem {prob_id}: {action}d '
                f'problem(s) {[problem.id for problem in problems]}, '
                f'statement {"imported" if statement is not None else "not found"}')

    problem_results = []
    for problem in problems:
        problem_result = {'id': problem.id, 'name': problem.name}
        if statement is not None:
            # stored by pynformatics in /moodle_probpics/<problem_id>/
            problem_result['images'] = {
                name: base64.b64encode(data).decode('ascii')
                for name, data in statement.images.items()
            }
        problem_results.append(problem_result)

    return {
        'action': action,
        'problems': problem_results,
        'statement': 'imported' if statement is not None else 'not found',
        'missing_images': statement.missing_images if statement is not None else [],
        'ejudge_problem': {
            key: ejudge_problem.get(key)
            for key in ('id', 'short_name', 'long_name', 'internal_name', 'extid', 'uuid')
        },
    }
