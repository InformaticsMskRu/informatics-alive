import datetime
import base64

from flask import (
    current_app,
    request,
)
from flask import jsonify as flask_jsonify
from flask.views import MethodView
from marshmallow import fields
from sqlalchemy import desc, true
from sqlalchemy.orm import Load
from webargs.flaskparser import parser
from werkzeug.exceptions import BadRequest, NotFound

from rmatics.ejudge.submit_queue import (
    queue_submit,
)
from rmatics.model import CourseModule
from rmatics.model.base import db
from rmatics.model.group import UserGroup
from rmatics.model.problem import Problem, EjudgeProblem
from rmatics.model.run import Run
from rmatics.model.user import SimpleUser
from rmatics.utils.response import jsonify
from rmatics.view import get_problems_by_statement_id
from rmatics.view.problem.serializers.problem import ProblemSchema
from rmatics.view.problem.serializers.run import RunSchema

DEFAULT_MOODLE_CONTEXT_SOURCE = 10


class TrustedSubmitApi(MethodView):
    post_args = {
        'lang_id': fields.Integer(required=True),
        'statement_id': fields.Integer(),
        'user_id': fields.Integer(required=True),

        # Submission context arguments.
        # By default all submission context parameters are optional
        # to preserve backward compatibility with Moodle handlers
        'context_id': fields.Integer(required=False),
        'context_source': fields.Integer(required=False, missing=DEFAULT_MOODLE_CONTEXT_SOURCE),
        'is_visible': fields.Boolean(required=False, missing=True),
    }

    @staticmethod
    def check_file_restriction(file, max_size_kb: int = 64) -> bytes:
        """ Function for checking submission restricts
            Checks only size (KB less then max_size_kb)
                and that is is not empty (len > 2)
            Raises
            --------
            ValueError if restriction is failed
        """
        max_size = max_size_kb * 1024
        file_bytes: bytes = file.read(max_size)
        if len(file_bytes) == max_size:
            raise ValueError('Submission should be less than 64Kb')
        # TODO: 4 это просто так, что такое пустой файл для ejudge?
        if len(file_bytes) < 4:
            raise ValueError('Submission shouldn\'t be empty')

        return file_bytes

    def post(self, problem_id: int):
        args = parser.parse(self.post_args)

        language_id = args['lang_id']
        statement_id = args.get('statement_id')
        user_id = args.get('user_id')
        file = parser.parse_files(request, 'file', 'file')

        # If context parameters are unavialable,
        # consider it as Moodle submission and set defaults
        context_id = args.get('context_id')
        context_source = args.get('context_source', DEFAULT_MOODLE_CONTEXT_SOURCE)
        is_visible = args.get('is_visible', True)

        # Здесь НЕЛЬЗЯ использовать .get(problem_id), см EjudgeProblem.__doc__
        problem = db.session.query(EjudgeProblem) \
            .filter_by(id=problem_id) \
            .one_or_none()

        if not problem:
            raise NotFound('Problem with this id is not found')

        if int(user_id) <= 0:
            raise BadRequest('Wrong user status')

        try:
            limit = 64
            if problem.output_only:
                limit = 1024 * 16
            text = self.check_file_restriction(file, limit)
        except ValueError as e:
            raise BadRequest(e.args[0])
        source_hash = Run.generate_source_hash(text)

        duplicate: Run = db.session.query(Run).filter(Run.user_id == user_id) \
            .filter(Run.problem_id == problem_id) \
            .order_by(Run.id.desc()).first()

        if duplicate is not None and \
                duplicate.source_hash == source_hash and \
                duplicate.ejudge_language_id == language_id:
            raise BadRequest('Source file is duplicate of your previous submission')

        # There is not constraint on statement_id
        run = Run(
            user_id=user_id,
            problem_id=problem_id,
            statement_id=statement_id,
            ejudge_contest_id=problem.ejudge_contest_id,
            ejudge_language_id=language_id,
            ejudge_status=377,  # In queue
            source_hash=source_hash,

            # Context related properties
            context_source=context_source,
            is_visible=is_visible,
        )
        # If it's context aware submission,
        # overwrite statement_id with context
        if context_id:
            run.statement_id = context_id

        db.session.add(run)
        db.session.flush()

        run.update_source(text)

        run_id = run.id
        ejudge_url = current_app.config['EJUDGE_NEW_CLIENT_URL']

        # Коммит должен быть до отправки в очередь иначе это гонка
        db.session.commit()

        queue_submit(run_id, ejudge_url)
        return jsonify({
            'run_id': run_id
        })


class ProblemApi(MethodView):
    def get(self, problem_id: int):
        problem = db.session.query(EjudgeProblem).get(problem_id)
        if not problem:
            raise NotFound('Problem with this id is not found')

        if not problem.sample_tests:
            schema = ProblemSchema(exclude=['sample_tests_json'])
        else:
            schema = ProblemSchema()

        data = schema.dump(problem)
        return jsonify(data.data)


get_args = {
    'user_id': fields.Integer(),
    'group_id': fields.Integer(),
    'lang_id': fields.Integer(),
    'status_id': fields.Integer(missing=-1, default=-1),
    'count': fields.Integer(default=10, missing=10),
    'page': fields.Integer(required=True),
    'from_timestamp': fields.Integer(),  # Может быть -1, тогда не фильтруем
    'to_timestamp': fields.Integer(),  # Может быть -1, тогда не фильтруем

    # Internal context scope arguments
    'context_id': fields.Integer(required=False),
    'context_source': fields.Integer(required=False),
    'show_hidden': fields.Boolean(required=False, missing=False, default=False),
    'include_source': fields.Boolean(required=False, missing=False, default=False),
}


# TODO: only teacher
class ProblemSubmissionsFilterApi(MethodView):
    """ View for getting problem submissions
        Possible filters
        ----------------
        from_timestamp: timestamp
        to_timestamp: timestamp
        group_id: int
        user_id: int
        lang_id: int
        status_id: int
        statement_id: int

        Returns
        --------
        'result': success | error
        'data': [Run]
        'metadata': {count: int, page_count: int}

        Also:
        --------
        If problem_id = 0 we are trying to find problems by
        CourseModule == statement_id
    """

    def get(self, problem_id: int):

        args = parser.parse(get_args, request)
        query = self._build_query_by_args(args, problem_id)
        per_page_count = args.get('count')
        page = args.get('page')
        result = query.paginate(page=page, per_page=per_page_count,
                                error_out=False, max_per_page=100)

        runs = []

        problem_ids = set()
        user_ids = set()

        for run in result.items:
            problem_ids.add(run.problem_id)
            user_ids.add(run.user_id)

        problems_result = db.session.query(Problem).filter(Problem.id.in_(problem_ids)).options(Load(Problem).load_only('id', 'name'))
        problems = dict()

        for problem in problems_result:
            problems[problem.id] = problem

        users_result = db.session.query(SimpleUser).filter(SimpleUser.id.in_(user_ids)).options(Load(SimpleUser).load_only('id', 'firstname', 'lastname'))
        users = dict()

        for u in users_result:
            users[u.id] = u

        for run in result.items:
            if run.user_id > 0:
                run.user = users[run.user_id]
                run.problem = problems[run.problem_id]
                if args.get('include_source'):
                    run.code = base64.b64encode(run.source)
                runs.append(run)

        metadata = {
            'count': result.total,
            'page_count': result.pages
        }

        schema = RunSchema(many=True)
        data = schema.dump(runs)

        return flask_jsonify({
            'result': 'success',
            'data': data.data,
            'metadata': metadata
        })

    @classmethod
    def _build_query_by_args(cls, args, problem_id):
        user_id = args.get('user_id')
        group_id = args.get('group_id')
        lang_id = args.get('lang_id')
        status_id = args.get('status_id')
        # Волшебные костыли, если problem_id == 0,
        # то statement_id - это CourseModule.id, а не Statement.id
        statement_id = request.args.get('statement_id', type=int, default=None)
        from_timestamp = args.get('from_timestamp')
        to_timestamp = args.get('to_timestamp')

        # Context arguments
        context_id = args.get('context_id')
        context_source = args.get('context_source')
        show_hidden = args.get('show_hidden')

        try:
            from_timestamp = from_timestamp and from_timestamp != -1 and \
                             datetime.datetime.fromtimestamp(from_timestamp / 1_000)
            to_timestamp = to_timestamp and to_timestamp != -1 and \
                           datetime.datetime.fromtimestamp(to_timestamp / 1_000)
        except (OSError, OverflowError, ValueError):
            raise BadRequest('Bad timestamp data')

        query = db.session.query(Run) \
            .order_by(desc(Run.id))
        if user_id:
            query = query.filter(Run.user_id == user_id)

        if group_id:
            user_subquery = db.session.query(SimpleUser.id.label('user_ids')) \
                .join(UserGroup, UserGroup.user_id == SimpleUser.id) \
                .filter(UserGroup.group_id == group_id) \
                .subquery('user_subquery')

            query = query.filter(Run.user_id == user_subquery.c.user_ids)

        if lang_id and lang_id > 0:
            query = query.filter(Run.ejudge_language_id == lang_id)
        if status_id != -1:
            query = query.filter(Run.ejudge_status == status_id)
        if from_timestamp:
            query = query.filter(Run.create_time > from_timestamp)
        if to_timestamp:
            query = query.filter(Run.create_time < to_timestamp)

        problem_id_filter_smt = None
        if problem_id != 0:
            problem_id_filter_smt = Run.problem_id == problem_id
            if statement_id:
                query = query.filter(Run.statement_id == statement_id)
        elif statement_id:
            # If problem_id == 0 filter by all problems from contest
            statement_id = db.session.query(CourseModule) \
                .filter(CourseModule.id == statement_id) \
                .one_or_none() \
                .instance \
                .id
            problems = get_problems_by_statement_id(statement_id)
            problem_ids = [problem.id for problem in problems]
            problem_id_filter_smt = Run.problem_id.in_(problem_ids)

        if problem_id == 0 and not statement_id and not group_id and not user_id:
            raise NotFound('You must specify at least problem_id or statement_id or group_id or user_id')
        if problem_id_filter_smt is not None:
            query = query.filter(problem_id_filter_smt)

        # Apply context filters
        if context_id is not None:
            query = query.filter(Run.statement_id == context_id)
        if context_source is not None:
            query = query.filter(Run.context_source == context_source)
        # If no visibility context supplied or explicitly set to False,
        # assume it's Moodle request and return only public submissions.
        # Otherwise, return ALL submissions
        if show_hidden is False:
            query = query.filter(Run.is_visible == true())

        return query
