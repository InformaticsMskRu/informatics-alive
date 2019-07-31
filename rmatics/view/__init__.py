from typing import Union, Dict

from flask import jsonify
from sqlalchemy.orm import Load

from rmatics import db
from rmatics.model import Problem, StatementProblem

DEFAULT_MESSAGE = (
    'Oops! An error happened. We are already '
    'trying to resolve the problem!'
)


def handle_api_exception(api_exception):
    code = getattr(api_exception, 'code', 500)
    if not isinstance(code, int):
        code = 500
    message = getattr(api_exception, 'description', DEFAULT_MESSAGE)
    response = jsonify(status='error', code=code, error=message)
    response.status_code = code
    return response


def get_problems_by_statement_id(statement_id: int, filter_hidden=True) -> list:
    """ Get all problems from is's id
         SELECT
            mdl_problems.id,
            mdl_statements_problems_correlation.rank,
            mdl_problems.name,
            mdl_statements_problems_correlation.hidden as cur_hidden,
            mdl_ejudge_problem.short_id,
            mdl_problems.pr_id,
            mdl_ejudge_problem.contest_id
        FROM
            mdl_problems, mdl_statements_problems_correlation, mdl_ejudge_problem
        WHERE
            mdl_ejudge_problem.id = mdl_problems.pr_id AND
            mdl_statements_problems_correlation.problem_id = mdl_problems.id AND
            mdl_statements_problems_correlation.statement_id = 928
        ORDER BY
            mdl_statements_problems_correlation.rank
    """

    problems_statement_problems = db.session.query(Problem, StatementProblem) \
        .join(StatementProblem, StatementProblem.problem_id == Problem.id) \
        .filter(StatementProblem.statement_id == statement_id) \
        .options(Load(Problem).load_only('id', 'name')) \
        .options(Load(StatementProblem).load_only('rank'))

    if filter_hidden:
        problems_statement_problems = problems_statement_problems.filter(StatementProblem.hidden != 1)

    problems = []
    # Yes it is ugly but I think its better than rewrite query
    for problem, sp in problems_statement_problems.all():
        problem.rank = sp.rank
        problems.append(problem)

    return problems
