from flask import Blueprint

from rmatics.view.contest.ejudge import ReloadProblemApi

contest_blueprint = Blueprint('contest', __name__, url_prefix='/contest')

contest_blueprint.add_url_rule('/ejudge/<int:judge_id>/reload/<int:contest_id>/<int:problem_id>',
                               methods=('POST', ),
                               view_func=ReloadProblemApi.as_view('ejudge_reload_problem'))
