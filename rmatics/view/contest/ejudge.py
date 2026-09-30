from flask import current_app
from flask.views import MethodView
from werkzeug.exceptions import BadGateway, NotFound

from rmatics.ejudge.ejudge_api import EjudgeApiError, EjudgeNotFound
from rmatics.ejudge.judges_config import get_judge
from rmatics.ejudge.problem_import import import_problem
from rmatics.utils.response import jsonify


class ReloadProblemApi(MethodView):
    """Import ejudge problem problem_id of contest_id on judge judge_id
    through the ejudge API (see rmatics.ejudge.problem_import).

    Called by pynformatics, which checks the moodle capability.
    """
    def post(self, judge_id: int, contest_id: int, problem_id: int):
        judge = get_judge(judge_id)
        if judge is None:
            raise NotFound(f'Judge {judge_id} is not configured')

        try:
            result = import_problem(judge, judge_id, contest_id, problem_id)
        except EjudgeNotFound as e:
            raise NotFound(str(e))
        except EjudgeApiError as e:
            current_app.logger.warning(
                f'Reload of problem {problem_id} of contest {contest_id} '
                f'on judge {judge_id} failed: {e}')
            raise BadGateway(str(e))
        return jsonify(result)
