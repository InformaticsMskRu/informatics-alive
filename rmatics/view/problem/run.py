import functools

from celery import shared_task
from celery.utils.log import get_task_logger

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
from rmatics.ejudge.submit_queue.task import submit_task
from rmatics.model.base import db, mongo
from rmatics.model.rejudge import Rejudge
from rmatics.model.run import Run
from rmatics.utils.cacher.helpers import invalidate_monitor_cache_by_run
from rmatics.utils.response import jsonify
from rmatics.view.problem.serializers.run import RunSchema

from rmatics.ejudge.protocol import fetch_protocol

from rmatics.utils.run import EjudgeStatuses

do_once = functools.lru_cache(1)

logger = get_task_logger(__name__)

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

        submit_task.delay(run.id)

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

@shared_task(name='rmatics.view.problem.run.upd_run', bind=True, max_retries=None, retry_backoff=True)
def upd_run(self, data) -> dict:
    ejudge_run_uuid = data['run_uuid']
    ejudge_run_id = int(data['run_id'])
    ejudge_contest_id = int(data['contest_id'])
    status = int(data['status'])
    try:
        judge_id = int(data['judge_id'])
    except:
        msg = f'Incorrect judge_id'
        raise BadRequest(msg)
    
    try:
        run = db.session.query(Run) \
            .filter_by(ejudge_run_uuid=ejudge_run_uuid,
                       judge_id=judge_id) \
            .one_or_none()
    except:
        msg = f'Cannot find Run with  \
                judge_id={judge_id},  \
                ejudge_run_uuid={ejudge_run_uuid}. Retry...'
        logger.exception(msg)
        self.retry(exc=BadRequest(msg), countdown=2 * self.request.retries)

    if run is None:
        msg = f'Cannot find Run with  \
                judge_id={judge_id},  \
                ejudge_run_uuid={ejudge_run_uuid}. Retry...'
        logger.exception(msg)
        self.retry(exc=BadRequest(msg), countdown=2 * self.request.retries)

    run_schema = FromEjudgeRunSchema(context={'instance': run})
    received_run, errors = run_schema.load(data)
    if errors:
        logger.exception("Failed to load schema. Retry...")
        self.retry(exc=BadRequest(errors), countdown=2 * self.request.retries)

    db.session.add(received_run)
    db.session.commit()  

    return {
        "judge_id": judge_id,
        "ejudge_contest_id": ejudge_contest_id,
        "ejudge_run_id": ejudge_run_id,
        "ejudge_run_uuid": ejudge_run_uuid,
        "run_id": int(run.id),
        "status": status
    }

@shared_task(name='rmatics.view.problem.run.load_protocol', bind=True, default_retry_delay=30, max_retries=5)
def load_protocol(self, data):

    try:
        run = db.session.query(Run) \
            .filter_by(ejudge_run_uuid=data["ejudge_run_uuid"],
                       judge_id=data["judge_id"]) \
            .one_or_none()
    except:
        msg = f'Cannot find Run with  \
                judge_id={data["judge_id"]},  \
                ejudge_run_uuid={data["ejudge_run_uuid"]}. Retry...'
        logger.exception(msg)
        raise BadRequest(msg)
    
    if run is None:
        msg = f'Cannot find Run with  \
                judge_id={data["judge_id"]},  \
                ejudge_run_uuid={data["ejudge_run_uuid"]}. Retry...'
        logger.exception(msg)
        raise BadRequest(msg)

    invalidate_monitor_cache_by_run(run)

    if _is_terminal(data["status"]):
        url, token = _resolve_judge(data["judge_id"])
        try:
            protocol = fetch_protocol(url, token, data["ejudge_contest_id"], data["ejudge_run_id"], data["run_id"])
            if protocol is not None:
                run.protocol = protocol
        except Exception as exc:
            if self.request.retries < load_protocol.max_retries:
                logger.info('retry protocol')
                self.retry(exc=exc)
            logger.warning('Failed to load protocol Max retries count exceed. Aborting.'
                          f'Request args={data}')
            raise exc

def make_upd_chain():
    upd_chain = upd_run.s() | load_protocol.s()
    return upd_chain

class UpdateRunFromOldEjudgeAPI(MethodView):

    def post(self):
        data = request.get_json(force=True)

        upd_chain = make_upd_chain()
        result = upd_chain.delay(data)

        return jsonify({}, 200)
