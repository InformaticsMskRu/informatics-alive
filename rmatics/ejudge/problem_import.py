"""Import a problem from a judge through the ejudge API: create or update
the informatics problem and route it to that judge with judges_settings.

Only what the API exposes is imported (name, limits, type). The statement
isn't: a new problem gets a placeholder, an existing one keeps its own.
"""
import datetime
from typing import List

from sqlalchemy import Text, type_coerce

from rmatics.ejudge import ejudge_api
from rmatics.ejudge.judges_config import JudgeConfig, get_default_judge_id
from rmatics.model.base import db
from rmatics.model.ejudge_contest import EjudgeContest
from rmatics.model.problem import EjudgeProblem, Problem
from rmatics.utils.moodle import get_contest_str_id

NO_STATEMENT = '<p>Условие пока не опубликовано...</p>'


def problem_fields(ejudge_problem: dict) -> dict:
    """Problem values from the ejudge problem, derived the way the
    filesystem reload derives them from serve.cfg."""
    long_name = ejudge_problem.get('long_name') or ''
    if 'time_limit_millis' in ejudge_problem:
        timelimit = ejudge_problem['time_limit_millis'] / 1000
    else:
        timelimit = ejudge_problem.get('time_limit', -1)
    return {
        # a problem must not have an empty name
        'name': long_name or ' ',
        'ejudge_name': long_name,
        'short_id': ejudge_problem.get('short_name'),
        'timelimit': timelimit,
        'memorylimit': ejudge_problem.get('max_vm_size'),
        'output_only': ejudge_problem.get('type') == 'output-only',
    }


def _routes_to(entry, judge_id: int, contest_id: int, prob_id: int) -> bool:
    if not isinstance(entry, dict):
        return False
    try:
        key = (int(entry['judge_id']), int(entry['contest_id']), int(entry['problem_id']))
    except (KeyError, TypeError, ValueError):
        return False
    return key == (judge_id, contest_id, prob_id)


def find_imported_problems(judge_id: int, contest_id: int, prob_id: int) -> List[EjudgeProblem]:
    """Problems already imported from ejudge problem prob_id of contest_id
    on judge_id.

    Such a problem has a judges_settings entry routing to that ejudge
    problem. Problems of the default judge loaded by the filesystem reload
    have no judges_settings and are found by the legacy
    ejudge_contest_id/problem_id instead.
    """
    # MariaDB 5.5 has no JSON functions: the contest id digits pick the
    # candidate rows, their entries are compared after parsing
    settings_text = type_coerce(EjudgeProblem.judges_settings, Text)
    candidates = db.session.query(EjudgeProblem) \
        .filter(settings_text.like(f'%{contest_id}%')) \
        .order_by(EjudgeProblem.id) \
        .all()
    found = [
        problem for problem in candidates
        if isinstance(problem.judges_settings, list)
        and any(_routes_to(entry, judge_id, contest_id, prob_id)
                for entry in problem.judges_settings)
    ]

    if judge_id == get_default_judge_id():
        legacy = db.session.query(EjudgeProblem) \
            .filter(EjudgeProblem.ejudge_contest_id == contest_id,
                    EjudgeProblem.problem_id == prob_id) \
            .order_by(EjudgeProblem.id) \
            .all()
        # a problem with judges_settings is routed by them, even if its
        # legacy columns still name this problem
        found += [problem for problem in legacy
                  if not problem.judges_settings and problem not in found]
    return found


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
        'short_id': fields['short_id'],
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
    (see find_imported_problems); an imported problem without
    judges_settings gets the entry routing to this ejudge problem."""
    ejudge_problem = ejudge_api.get_problem(judge, contest_id, prob_id)
    return _import(judge, judge_id, contest_id, prob_id, ejudge_problem)


def import_contest(judge: JudgeConfig, judge_id: int, contest_id: int) -> dict:
    """import_problem for every problem of the contest.

    Each problem is committed on its own: a failure leaves the problems
    before it imported.
    """
    results = []
    for ejudge_problem in ejudge_api.list_problems(judge, contest_id):
        prob_id = ejudge_problem.get('id')
        if not isinstance(prob_id, int):
            raise ejudge_api.EjudgeApiError(
                f'list-problems-json failed: problem without id: {ejudge_problem!r}')
        results.append(_import(judge, judge_id, contest_id, prob_id, ejudge_problem))
    return {'problems': results}


def _import(judge: JudgeConfig, judge_id: int, contest_id: int, prob_id: int,
            ejudge_problem: dict) -> dict:
    fields = problem_fields(ejudge_problem)
    entry = {'judge_id': judge_id, 'contest_id': contest_id, 'problem_id': prob_id}

    problems = find_imported_problems(judge_id, contest_id, prob_id)
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
    db.session.commit()

    return {
        'action': action,
        'problems': [{'id': problem.id, 'name': problem.name} for problem in problems],
        'ejudge_problem': {
            key: ejudge_problem.get(key)
            for key in ('id', 'short_name', 'long_name', 'internal_name', 'extid', 'uuid')
        },
    }
