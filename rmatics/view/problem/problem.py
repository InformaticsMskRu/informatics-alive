import datetime

from flask import (
    current_app,
    request,
)
from flask import jsonify as flask_jsonify
from sqlalchemy.orm import Load
from werkzeug.exceptions import BadRequest, NotFound
from flask.views import MethodView
from rmatics.ejudge.submit_queue import (
    get_last_get_id,
    queue_submit,
)
from sqlalchemy import desc
from webargs.flaskparser import parser
from marshmallow import fields

from rmatics.model import CourseModule
from rmatics.model.base import db
from rmatics.model.group import UserGroup
from rmatics.model.problem import Problem, EjudgeProblem
from rmatics.model.run import Run
from rmatics.model.user import SimpleUser
from rmatics.utils.response import jsonify
from rmatics.view import get_problems_by_statement_id
from rmatics.view.problem.serializers.run import RunSchema

from rmatics.view.problem.serializers.problem import ProblemSchema


class TrustedSubmitApi(MethodView):
    post_args = {
        'lang_id': fields.Integer(required=True),
        'statement_id': fields.Integer(),
        'user_id': fields.Integer(required=True),
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

        # Здесь НЕЛЬЗЯ использовать .get(problem_id), см EjudgeProblem.__doc__
        problem = db.session.query(EjudgeProblem) \
            .filter_by(id=problem_id) \
            .one_or_none()

        if not problem:
            raise NotFound('Problem with this id is not found')

        try:
            text = self.check_file_restriction(file)
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

        # TODO: разобраться, есть ли там constraint на statement_id
        run = Run(
            user_id=user_id,
            problem_id=problem_id,
            statement_id=statement_id,
            ejudge_contest_id=problem.ejudge_contest_id,
            ejudge_language_id=language_id,
            ejudge_status=377,  # In queue
            source_hash=source_hash,
        )

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
        query = query.options(Load(Problem).load_only('id', 'name'),
                              Load(SimpleUser).load_only('id', 'firstname', 'lastname'))

        per_page_count = args.get('count')
        page = args.get('page')
        result = query.paginate(page=page, per_page=per_page_count,
                                error_out=False, max_per_page=100)

        runs = []
        for run, user, problem in result.items:
            run.user = user
            run.problem = problem
            runs.append(run)

        metadata = {
            'count': result.total,
            'page_count': result.pages
        }

        schema = RunSchema(many=True)
        data = schema.dump(runs)

        return flask_jsonify(
            {
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

        try:
            from_timestamp = from_timestamp and from_timestamp != -1 and \
                             datetime.datetime.fromtimestamp(from_timestamp / 1_000)
            to_timestamp = to_timestamp and to_timestamp != -1 and \
                           datetime.datetime.fromtimestamp(to_timestamp / 1_000)
        except (OSError, OverflowError, ValueError):
            raise BadRequest('Bad timestamp data')

        query = db.session.query(Run, SimpleUser, Problem) \
            .join(SimpleUser, SimpleUser.id == Run.user_id) \
            .join(Problem, Problem.id == Run.problem_id) \
            .order_by(desc(Run.id))
        if user_id:
            query = query.filter(Run.user_id == user_id)

        if group_id:
            user_subquery = db.session.query(SimpleUser.id.label('user_ids')) \
                .join(UserGroup, UserGroup.user_id == SimpleUser.id) \
                .filter(UserGroup.group_id == group_id) \
                .subquery('user_subquery')

            query = query.filter(SimpleUser.id == user_subquery.c.user_ids)

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
        if problem_id_filter_smt is not None:
            query = query.filter(problem_id_filter_smt)

        return query
