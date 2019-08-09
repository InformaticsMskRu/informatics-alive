import datetime
from collections import namedtuple, OrderedDict
from typing import Iterable, Tuple, Optional

from dateutil.tz import UTC
from flask import request
from flask.views import MethodView
from marshmallow import fields
from sqlalchemy import select, true
from webargs.flaskparser import parser

from rmatics import db, monitor_cacher
from rmatics.model import SimpleUser, UserGroup, CourseModule, Statement, MonitorCourseModule
from rmatics.model.monitor import MonitorStatement, Monitor
from rmatics.model.run import LightWeightRun
from rmatics.model.user import LightWeightUser
from rmatics.utils.response import jsonify
from rmatics.view import get_problems_by_statement_id
from rmatics.view.monitors.serializers.monitor import ContestBasedMonitorSchema, \
    ProblemBasedMonitorSchema

ContestBasedMonitorData = namedtuple('ContestBasedMonitorData', ('contest_id', 'problem', 'runs'))
ProblemBasedMonitorData = namedtuple('ProblemBasedMonitorData', ('problem_id', 'runs'))


@monitor_cacher
def get_runs(problem_id: int = None, user_ids: Iterable = None,
             time_after: int = None, time_before: int = None,
             context_id: int = None, context_source: int = None, show_hidden: bool = False):
    """ We are using SQLAlchemy Сore to speedup multiply object fetching and serializing """

    query = select([LightWeightRun, LightWeightUser]) \
        .select_from(LightWeightRun.join(LightWeightUser, LightWeightRun.c.user_id == LightWeightUser.c.id))

    if problem_id is not None:
        query = query.where(LightWeightRun.c.problem_id == problem_id)

    if user_ids is not None:
        query = query.where(LightWeightRun.c.user_id.in_(user_ids))

    if time_after is not None:
        time_after = datetime.datetime.fromtimestamp(time_after)
        query = query.where(LightWeightRun.c.create_time > time_after)
    if time_before is not None:
        time_before = datetime.datetime.fromtimestamp(time_before)
        query = query.where(LightWeightRun.c.create_time < time_before)

    # apply context filters
    if context_id is not None:
        query = query.where(LightWeightRun.c.statement_id == context_id)
    if context_source is not None:
        query = query.where(LightWeightRun.c.context_source == context_source)
    if show_hidden is False:
        query = query.where(LightWeightRun.c.is_visible == true())

    query = query.order_by(LightWeightRun.c.id)

    conn = db.engine.connect()
    result = conn.execute(query)

    data = [
        {
            'id': run[0],
            'user': {
                'id': run[7],
                'firstname': run[8],
                'lastname': run[9]
            },
            'problem_id': run[2],
            'create_time': run[3].replace(tzinfo=UTC).strftime('%Y-%m-%dT%H:%M:%S%z'),
            'ejudge_score': run[4],
            'ejudge_status': run[5],
            'ejudge_test_num': run[6],
        }
        for run in result]

    return data


contest_based_get_args = {
    'group_id': fields.Integer(missing=None),
    'contest_id': fields.List(fields.Integer(), required=True),
    'time_before': fields.Integer(missing=None),
    'time_after': fields.Integer(missing=None),

     # Internal context scope arguments
    'context_source': fields.Integer(required=False),
    'show_hidden': fields.Boolean(required=False, missing=False),
}


class ContestBasedMonitorAPIView(MethodView):
    def get(self):
        args = parser.parse(contest_based_get_args, request)

        course_module_ids = args['contest_id']
        group_id = args['group_id']
        time_before = args['time_before']
        time_after = args['time_after']

        # Context arguments
        context_source = args.get('context_source')
        show_hidden = args.get('show_hidden')

        contest_ids = []
        for cm_id in course_module_ids:
            monitor_group_id, statement_ids = self._get_contests(cm_id)
            contest_ids += statement_ids
            group_id = group_id or monitor_group_id

        if group_id:
            users = db.session.query(SimpleUser) \
                .join(UserGroup, UserGroup.user_id == SimpleUser.id) \
                .filter(UserGroup.group_id == group_id)
            user_ids = [user.id for user in users]
        else:
            user_ids = None

        contest_problems = OrderedDict()
        for contest_id in contest_ids:
            contest_problems[contest_id] = get_problems_by_statement_id(contest_id)

        contest_problems_runs = []
        for contest_id, problems in contest_problems.items():
            for problem in problems:
                runs = get_runs(problem_id=problem.id,
                                user_ids=user_ids,
                                time_before=time_before,
                                time_after=time_after,
                                context_source=context_source,
                                show_hidden=show_hidden
                                )
                monitor_data = ContestBasedMonitorData(contest_id, problem, runs)
                contest_problems_runs.append(monitor_data)

        schema = ContestBasedMonitorSchema(many=True)

        response = schema.dump(contest_problems_runs)

        # We have to commit session because we may created cache_meta
        db.session.commit()

        return jsonify(response.data)

    @classmethod
    def _get_contests(cls, course_module_id: int) -> Tuple[Optional[int], list]:
        """
        Returns
        -------
        if cm is Monitor:
            Monitor.group_id, [statement_id]
        if cm was Statement:
             None, [statement_id]
        """
        cm = CourseModule
        course_module = db.session.query(cm).filter(cm.id == course_module_id).one_or_none()
        if course_module is None:
            return None, []
        course_module_instance = course_module.instance

        if isinstance(course_module_instance, Statement):
            return None, [course_module_instance.id]

        elif isinstance(course_module_instance, MonitorCourseModule):
            statement_ids = db.session.query(MonitorStatement.statement_id) \
                .join(Monitor, Monitor.id == MonitorStatement.monitor_id) \
                .join(MonitorCourseModule, MonitorCourseModule.monitor_id == Monitor.id) \
                .filter(MonitorCourseModule.id == course_module_instance.id) \
                .order_by(MonitorStatement.sort) \
                .all()
            statement_ids = [s[0] for s in statement_ids]
            return course_module_instance.group_id, statement_ids

        return None, []


problem_based_get_args = {
    'user_id': fields.List(fields.Integer(), missing=None),
    'problem_id': fields.List(fields.Integer(), required=True),
    'time_before': fields.Integer(missing=None),
    'time_after': fields.Integer(missing=None),

    # Internal context scope arguments
    'context_id': fields.Integer(required=False),
    'context_source': fields.Integer(required=False),
    'show_hidden': fields.Boolean(required=False),
}


class ProblemBasedMonitorAPIView(MethodView):
    """ This view if for splitting Monitors and moodle db
        We would like avoid Contests and MonitorStatements and Groups here
    """

    def get(self):
        args = parser.parse(problem_based_get_args, request)

        user_ids = args['user_id']
        problem_ids = args['problem_id']
        time_before = args['time_before']
        time_after = args['time_after']

        # Context arguments
        context_id = args.get('context_id')
        context_source = args.get('context_source')
        show_hidden = args.get('show_hidden')

        problem_runs = []
        for problem_id in problem_ids:
            runs = get_runs(problem_id=problem_id,
                            user_ids=user_ids,
                            time_before=time_before,
                            time_after=time_after,

                            context_id=context_id,
                            context_source=context_source,
                            show_hidden=show_hidden, )
            problem_runs.append(ProblemBasedMonitorData(problem_id, runs))

        schema = ProblemBasedMonitorSchema(many=True)
        problem_runs = schema.dump(problem_runs)

        # We have to commit session because we may created cache_meta
        db.session.commit()

        return jsonify(problem_runs.data)
