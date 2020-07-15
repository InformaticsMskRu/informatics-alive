import datetime
import hashlib
import logging
from typing import Optional

from flask import g
from sqlalchemy import MetaData, Table

from rmatics.model.base import db, mongo
from rmatics.utils.decorators import deprecated
from rmatics.utils.functions import attrs_to_dict

EJUDGE_COLUMNS = [
    'run_id',
    'contest_id',
    'run_uuid',
    'score',
    'status',
    'lang_id',
    'test_num',
    'create_time',
    'last_change_time',
]


class Run(db.Model):
    __table_args__ = (
        {'schema': 'pynformatics'},
    )
    __tablename__ = 'runs'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_user.id'))
    problem_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_problems.id'))
    statement_id = db.Column(db.Integer)
    score = db.Column(db.Integer)

    user = db.relationship('SimpleUser', backref='runs', lazy='select')
    problem = db.relationship('EjudgeProblem', backref=db.backref('runs', lazy='dynamic'))
    # statement = db.relationship('Statement', backref='runs')

    create_time = db.Column(db.DateTime(), default=datetime.datetime.utcnow)

    # Поля скопированные из ejudge.runs
    ejudge_run_id = db.Column('ej_run_id', db.Integer)
    ejudge_contest_id = db.Column('ej_contest_id', db.Integer)
    ejudge_run_uuid = db.Column('ej_run_uuid', db.String(40))

    ejudge_score = db.Column('ej_score', db.Integer)
    ejudge_status = db.Column('ej_status', db.Integer)
    ejudge_language_id = db.Column('ej_lang_id', db.Integer)
    ejudge_test_num = db.Column('ej_test_num', db.Integer)

    ejudge_create_time = db.Column('ej_create_time', db.DateTime)
    ejudge_last_change_time = db.Column('ej_last_change_time', db.DateTime)
    ejudge_url = db.Column(db.String(50))

    source_hash = db.Column(db.String(32))  # We are using md5 hex digest

    # Context fields
    # context_id = db.Column(db.Integer, nullable=True)
    context_source = db.Column(db.Integer, nullable=True)
    # Run is visible by default
    is_visible = db.Column(db.Boolean, nullable=True, default=True)

    def update_source(self, blob: bytes):
        mongo.db.source.insert_one({
            'run_id': self.id,
            'blob': blob,
        })
        return blob

    def remove_source(self):
        mongo.db.source.find_one_and_delete({'run_id': self.id})

    def move_protocol_to_rejudge_collection(self, rejudge_id: int):
        protocol = mongo.db.protocol.find_one({'run_id': self.id})
        if protocol:
            del protocol['_id']
            protocol['rejudge_id'] = rejudge_id
            mongo.db.rejudge.insert_one(protocol)
            mongo.db.protocol.find_one_and_delete({'run_id': self.id})

    @property
    def source(self) -> Optional[bytes]:
        data = mongo.db.source.find_one({'run_id': self.id})
        if data is None:
            logging.error(f'Cannot find source for run #{self.id}')
            return None
        blob = data.get('blob')
        return blob

    @property
    def protocol(self) -> Optional[dict]:
        return mongo.db.protocol.find_one({'run_id': self.id}, {'_id': False})

    @protocol.setter
    def protocol(self, protocol_source: dict):
        mongo.db.protocol.update({'run_id': self.id}, protocol_source, upsert=True)

    @staticmethod
    def generate_source_hash(blob: bytes) -> str:
        m = hashlib.md5()
        m.update(blob)
        return m.hexdigest()

    @property
    def status(self):
        return self.ejudge_status

    @property
    def language_id(self):
        return self.ejudge_language_id

    @staticmethod
    @deprecated
    def pick_ejudge_columns(ejudge_run):
        return {
            'ejudge_run_id': ejudge_run.run_id,
            'ejudge_contest_id': ejudge_run.contest_id,
            'ejudge_run_uuid': ejudge_run.run_uuid,
            'ejudge_score': ejudge_run.score,
            'ejudge_status': ejudge_run.status,
            'ejudge_language_id': ejudge_run.lang_id,
            'ejudge_test_num': ejudge_run.test_num,
            'ejudge_create_time': ejudge_run.create_time,
            'ejudge_last_change_time': ejudge_run.last_change_time,
        }

    def serialize(self, attributes=None):
        if attributes is None:
            attributes = (
                'id',
                'user',
                'problem_id',
                'statement_id',
                'score',
                'status',
                'language_id',
                'create_time',
                'ejudge_run_id',
                'ejudge_contest_id',
            )
        if hasattr(g, 'user') and g.user.id == self.user_id:
            attributes = (
                *attributes,
                'source',
            )
        serialized = attrs_to_dict(self, *attributes)

        if 'create_time' in attributes:
            serialized['create_time'] = str(self.create_time)

        if 'user' in attributes:
            serialized['user'] = self.user.serialize()

        return serialized


# SQLAlchemy Core table to avoiding class instance creation
LightWeightRun = Table(
    'runs', MetaData(),
    db.Column('id', db.Integer, primary_key=True),
    db.Column('user_id', None, db.ForeignKey('moodle.mdl_user.id')),
    db.Column('problem_id', db.Integer),
    db.Column('create_time', db.DateTime),
    db.Column('ej_score', db.Integer),
    db.Column('ej_status', db.Integer),
    db.Column('ej_test_num', db.Integer),

    # Need for context-related WHERE clause
    db.Column('statement_id', db.Integer),
    db.Column('context_source', db.Integer),
    db.Column('is_visible', db.Boolean),
    schema='pynformatics'
)
