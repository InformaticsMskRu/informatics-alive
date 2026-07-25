from flask import Blueprint

from rmatics.utils.auth import require_trusted_token
from rmatics.view.monitors.monitor import ContestBasedMonitorAPIView, ProblemBasedMonitorAPIView

monitor_blueprint = Blueprint('monitor', __name__, url_prefix='/monitor')

monitor_blueprint.add_url_rule('/', methods=('GET', 'POST'),
                               view_func=require_trusted_token(ContestBasedMonitorAPIView.as_view('crud')))


monitor_blueprint.add_url_rule('/problem_monitor', methods=('GET', ),
                               view_func=require_trusted_token(ProblemBasedMonitorAPIView.as_view('problem_monitor')))
