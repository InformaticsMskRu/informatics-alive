from flask import current_app
from flask.views import MethodView
from werkzeug.exceptions import BadGateway, Conflict, NotFound

from rmatics.ejudge.ejudge_api import EjudgeApiError, EjudgeNotFound
from rmatics.ejudge.judges_config import get_judge
from rmatics.ejudge.problem_import import ImportLocked, import_contest, import_problem
from rmatics.utils.response import jsonify


def _reload(judge_id: int, what: str, do_import):
    judge = get_judge(judge_id)
    if judge is None:
        raise NotFound(f'Judge {judge_id} is not configured')

    try:
        result = do_import(judge)
    except ImportLocked as e:
        raise Conflict(str(e))
    except EjudgeNotFound as e:
        raise NotFound(str(e))
    except EjudgeApiError as e:
        current_app.logger.warning(f'Reload of {what} on judge {judge_id} failed: {e}')
        raise BadGateway(str(e))
    return jsonify(result)


class ReloadProblemApi(MethodView):
    """Import ejudge problem problem_id of contest_id on judge judge_id
    through the ejudge API (see rmatics.ejudge.problem_import).

    Called by pynformatics, which checks the moodle capability.
    """
    def post(self, judge_id: int, contest_id: int, problem_id: int):
        return _reload(
            judge_id, f'problem {problem_id} of contest {contest_id}',
            lambda judge: import_problem(judge, judge_id, contest_id, problem_id))


class ReloadContestApi(MethodView):
    """ReloadProblemApi for every problem of the contest."""
    def post(self, judge_id: int, contest_id: int):
        return _reload(
            judge_id, f'contest {contest_id}',
            lambda judge: import_contest(judge, judge_id, contest_id))
