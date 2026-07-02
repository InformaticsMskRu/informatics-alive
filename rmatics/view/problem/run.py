from bson import ObjectId
from flask import request, current_app
from flask.views import MethodView
from marshmallow import fields, Schema, post_load
from pymongo.errors import PyMongoError, DuplicateKeyError
from webargs.flaskparser import parser
from werkzeug.exceptions import NotFound, BadRequest, InternalServerError
from sqlalchemy import or_
from typing import Optional

from rmatics.ejudge.judges_config import get_judge
from rmatics.ejudge.submit_queue import make_submit_task_chain
from rmatics.model.base import db, mongo
from rmatics.model.rejudge import Rejudge
from rmatics.model.run import Run
from rmatics.utils.cacher.helpers import invalidate_monitor_cache_by_run
from rmatics.utils.response import jsonify
from rmatics.view.problem.serializers.run import RunSchema

from rmatics.ejudge.protocol import fetch_protocol

from rmatics.utils.run import EjudgeStatuses

POSSIBLE_SOURCE_ENCODINGS = ['utf-8', 'cp1251', 'windows-1251', 'ascii', 'koi8-r']


class FromEjudgeRunSchema(Schema):
    score = fields.Integer()
    status = fields.Integer()
    lang_id = fields.Integer()
    test_num = fields.Integer()
    create_time = fields.DateTime()
    last_change_time = fields.DateTime()

    @post_load
    def load_ejudge_update(self, data: dict):
        run = self.context.get('instance')
        for k, v in data.items():
            setattr(run, f'ejudge_{k}', v)

        return run


class RunAPI(MethodView):
    def put(self, run_id: int):
        """ View for updating run """
        data = request.get_json(force=True, silent=False)

        run = db.session.query(Run).get(run_id)
        if run is None:
            raise NotFound(f'Run with id #{run_id} is not found')

        only_fields = ['ejudge_status', 'ejudge_test_num', 'ejudge_score']
        load_run_schema = RunSchema(only=only_fields, context={'instance': run})
        _, errors = load_run_schema.load(data)

        if errors:
            raise BadRequest(errors)

        invalidate_monitor_cache_by_run(run)

        # Avoid excess DB queries
        excludes = ['user', 'problem']
        dump_run_schema = RunSchema(exclude=excludes)
        data, _ = dump_run_schema.dump(run)

        db.session.add(run)
        db.session.commit()

        return jsonify(data)

    def post(self, run_id: int):
        """ View for rejudge run"""
        run: Run = db.session.query(Run).get(run_id)
        if run is None:
            raise NotFound(f'Run with id #{run_id} is not found')

        rejudge = Rejudge(run_id=run.id,
                          ejudge_contest_id=run.ejudge_contest_id)
        db.session.add(rejudge)
        db.session.flush([rejudge])

        run.move_protocol_to_rejudge_collection(rejudge.id)

        run.ejudge_status = 377
        run.ejudge_test_num = None
        run.ejudge_score = None
        run.ejudge_last_timestamp = 0
        db.session.add(run)
        db.session.commit()

        submit_task_chain = make_submit_task_chain()
        submit_task_chain.delay(run.id)

        return jsonify({})


class SourceApi(MethodView):

    get_args = {
        'is_admin': fields.Boolean(default=False, missing=False),
        'user_id': fields.Integer(),
        'context_source': fields.Integer(default=0),
    }

    def get(self, run_id: int):
        args = parser.parse(self.get_args, request)
        is_admin = args.get('is_admin')
        user_id = args.get('user_id')
        context_source = args.get('context_source')

        run_q = db.session.query(Run)

        if context_source and context_source > 0 and not is_admin:
            run_q = run_q.filter(or_(Run.context_source == context_source, Run.user_id == user_id))
        elif not is_admin:
            run_q = run_q.filter(Run.user_id == user_id)

        run = run_q.filter(Run.id == run_id).one_or_none()

        if run is None:
            raise NotFound(f'Run with id #{run_id} is not found')

        language_id = run.lang_id

        source = run.source or b''
        for encoding in POSSIBLE_SOURCE_ENCODINGS:
            try:
                source = source.decode(encoding)
                break
            except UnicodeDecodeError:
                pass

        return jsonify({'source': source, 'language_id': language_id})


class ProtocolApi(MethodView):

    get_args = {
        'is_admin': fields.Boolean(default=False, missing=False),
        'user_id': fields.Integer(),
        'context_source': fields.Integer(default=0),
    }

    def get(self, run_id: int):
        args = parser.parse(self.get_args, request)
        is_admin = args.get('is_admin')
        user_id = args.get('user_id')
        context_source = args.get('context_source')

        run_q = db.session.query(Run)

        if context_source and context_source > 0 and not is_admin:
            run_q = run_q.filter(or_(Run.context_source == context_source, Run.user_id == user_id))
        elif not is_admin:
            run_q = run_q.filter(Run.user_id == user_id)

        run = run_q.filter(Run.id == run_id).one_or_none()

        if run is None:
            raise NotFound(f'Run with id #{run_id} is not found')

        protocol = run.protocol
        if not protocol:
            raise NotFound(f'Protocol for run_id: {run_id} not found')

        return jsonify(protocol)

NON_TERMINAL_STATUSES = {
    EjudgeStatuses.COMPILING.value,  # 98
    EjudgeStatuses.RUNNING.value,    # 96
    EjudgeStatuses.IN_QUEUE.value,   # 377
}

def _is_terminal(status: Optional[int]) -> bool:
    return status is not None and status not in NON_TERMINAL_STATUSES

def _resolve_judge(judge_id: int):
    """(url, token) ejudge'а, у которого спрашивать протокол."""
    judge = get_judge(judge_id)
    return judge.url, judge.get_token()

class UpdateRunFromOldEjudgeAPI(MethodView):

    def post(self):
        data = request.get_json(force=True)
        judge_id = data['judge_id']
        ejudge_run_id = data['run_id']
        ejudge_contest_id = data['contest_id']
        status = data['status']

        run = db.session.query(Run) \
            .filter_by(ejudge_run_id=ejudge_run_id,
                       ejudge_contest_id=ejudge_contest_id) \
            .one_or_none()

        if run is None:
            msg = f'Cannot find Run with  \
                    ejudge_contest_id={ejudge_contest_id},  \
                    ejudge_run_id={ejudge_run_id}'
            raise BadRequest(msg)

        run_schema = FromEjudgeRunSchema(context={'instance': run})
        received_run, errors = run_schema.load(data)
        if errors:
            raise BadRequest(errors)

        db.session.add(received_run)
        db.session.commit()

        invalidate_monitor_cache_by_run(run)

        if _is_terminal(status):
            url, token = _resolve_judge(judge_id)
            try:
                protocol = fetch_protocol(url, token, ejudge_contest_id, ejudge_run_id, run.id)
                if protocol is not None:
                    run.protocol = protocol
            except:
                raise BadRequest("Protocol was not found")

        return jsonify({}, 200)
