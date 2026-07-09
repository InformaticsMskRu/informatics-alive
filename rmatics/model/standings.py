import logging
from sqlalchemy import and_
from sqlalchemy.ext.declarative import declared_attr

from rmatics.model.base import db
from rmatics.model.ejudge_run import EjudgeRun
from rmatics.model.group import Group
from rmatics.model.user import SimpleUser
from rmatics.utils.functions import (
    attrs_to_dict, 
    index_of,
)
from rmatics.utils.json_type import (
    JsonType,
    MutableDict,
)


log = logging.getLogger(__name__)


class StandingsMixin:
    __table_args__ = {'schema': 'pynformatics'}

    @declared_attr
    def json(cls):
        return db.Column('json', MutableDict.as_mutable(JsonType))

    def update(self, user):
        if not self.json:
            self.json = {}

        if str(user.id) not in self.json:
            self.json[str(user.id)] = {
                **attrs_to_dict(user, 'firstname', 'lastname'),
            }


class ProblemStandings(StandingsMixin, db.Model):
    __tablename__ = 'problem_standings'
    __table_args__ = {'schema': 'pynformatics'}


    problem_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_problems.id'), primary_key=True)
    problem = db.relationship('EjudgeProblem', backref=db.backref('standings', uselist=False, lazy='joined'))

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        db.session.add(instance)

        # Flush, чтобы получить из базы problem
        db.session.flush([instance])

        # Expire, чтобы у задачи стал доступен standings
        db.session.expire(instance.problem)

        log.info('ProblemStandings(problem_id=%s) Created. Starting updates' % instance.problem_id)

        users = db.session.query(SimpleUser) \
            .join(EjudgeRun) \
            .filter(
                and_(
                    EjudgeRun.contest_id == instance.problem.ejudge_contest_id,
                    EjudgeRun.prob_id == instance.problem.problem_id
                )
            ) \
            .distinct() \
            .all()

        with db.session.no_autoflush:
            for i, user in enumerate(users):
                instance.update(user)

        log.info('ProblemStandings(problem_id=%s) Updates finished.' % instance.problem_id)

        return instance

    def update(self, user):
        super(ProblemStandings, self).update(user)

        user_runs = self.problem.ejudge_runs \
            .filter_by(user_id=user.ejudge_id) \
            .order_by(EjudgeRun.create_time) \
            .all()

        processed = {
            'attempts': 0,
            'score': 0,
            'status': None,
        }
        for run in user_runs:
            processed['attempts'] += 1
            if run.score > processed['score']:
                processed['score'] = run.score
                processed['status'] = run.status

            if run.score == 100:
                break

        self.json[user.id].update(processed)

    def serialize(self):
        return self.json


class StatementStandings(StandingsMixin, db.Model):
    __tablename__ = 'statement_standings'

    statement_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_statements.id'), primary_key=True)
    statement = db.relationship('Statement', backref=db.backref('standings', uselist=False, lazy='joined'))

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        db.session.add(instance)

        # Flush, чтобы получить из базы statement
        db.session.flush([instance])

        # Expire, чтобы у statement стал доступен standings
        db.session.expire(instance.statement)

        log.info('StatementStandings(statement_id=%s) Created. Starting updates' % instance.statement_id)

        pynformatics_runs = db.session.query(PynformaticsRun) \
            .filter_by(statement_id=instance.statement_id) \
            .all()

        with db.session.no_autoflush:
            for pynformatics_run in pynformatics_runs:
                instance.update(pynformatics_run.run)

        log.info('StatementStandings(statement_id=%s) Updates finished.' % instance.statement_id)

        return instance

    def update(self, run):
        user = run.user
        super(StatementStandings, self).update(user)

        runs = self.json[str(user.id)].get('runs', [])

        replace_index = index_of(
            runs,
            lambda run_json: run_json['run_id'] == run.run_id and run_json['contest_id'] == run.contest_id
        )

        if replace_index is not None:
            runs[replace_index] = StatementStandings.serialize_run(run)
        else:
            insert_index = index_of(
                runs,
                lambda run_json: run_json['create_time'] > run.create_time.isoformat(),
                len(runs)
            )
            runs.insert(insert_index, StatementStandings.serialize_run(run))

        self.json[str(user.id)]['runs'] = runs
        self.json.changed()

    # TODO: добавить обработку настроек контеста
    def serialize(self, group_id=None):
        result = self.json
        if group_id:
            try:
                group = db.session.query(Group).filter_by(id=group_id).one()
            except Exception:
                raise ValueError('Group not found')

            result = {
                str(user_group.user_id): result[str(user_group.user_id)]
                for user_group in group.user_groups
                if str(user_group.user_id) in result
            }

        return result
