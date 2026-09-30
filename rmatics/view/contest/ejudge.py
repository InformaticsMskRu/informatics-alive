from flask.views import MethodView
from werkzeug.exceptions import NotFound

from rmatics.ejudge.ejudge_api import EjudgeApiError, EjudgeNotFound
from rmatics.ejudge.import_log import ImportLog
from rmatics.ejudge.judges_config import get_judge
from rmatics.ejudge.problem_import import ImportLocked, import_contest, import_problem
from rmatics.utils.response import jsonify


def _reload(judge_id: int, contest_id: int, do_import):
    """Run the import; the reply carries its log, a failure's too."""
    judge = get_judge(judge_id)
    if judge is None:
        raise NotFound(f'Judge {judge_id} is not configured')

    log = ImportLog(judge_id, contest_id)
    try:
        result = do_import(judge, log)
    except ImportLocked as e:
        return jsonify({'message': str(e), 'log': log.lines}, status_code=409)
    except EjudgeNotFound as e:
        log.warning(f'failed: {e}')
        return jsonify({'message': str(e), 'log': log.lines}, status_code=404)
    except EjudgeApiError as e:
        log.warning(f'failed: {e}')
        return jsonify({'message': str(e), 'log': log.lines}, status_code=502)
    log.info('done')
    result['log'] = log.lines
    return jsonify(result)


class ReloadProblemApi(MethodView):
    """Import ejudge problem problem_id of contest_id on judge judge_id
    through the ejudge API (see rmatics.ejudge.problem_import).

    Called by pynformatics, which checks the moodle capability.
    """
    def post(self, judge_id: int, contest_id: int, problem_id: int):
        return _reload(
            judge_id, contest_id,
            lambda judge, log: import_problem(judge, judge_id, contest_id, problem_id, log))


class ReloadContestApi(MethodView):
    """ReloadProblemApi for every problem of the contest."""
    def post(self, judge_id: int, contest_id: int):
        return _reload(
            judge_id, contest_id,
            lambda judge, log: import_contest(judge, judge_id, contest_id, log))
