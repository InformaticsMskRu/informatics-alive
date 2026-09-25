import time
from flask import g
from jsonschema import Draft4Validator
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.exc import NoResultFound

from rmatics.model.base import db
from rmatics.model.course_module import CourseModuleInstance

from rmatics.utils.functions import attrs_to_dict
from rmatics.utils.json_type import JsonType


class Statement(CourseModuleInstance, db.Model):
    __table_args__ = {'schema': 'moodle'}
    __tablename__ = 'mdl_statements'
    __mapper_args__ = {
        'polymorphic_identity': 'statement',
        'concrete': True,
    }

    MODULE = 19

    id = db.Column(db.Integer, primary_key=True)
    course_id = db.Column('course', db.Integer, db.ForeignKey('moodle.mdl_course.id'))
    name = db.Column(db.Unicode(255))
    summary = db.Column(MEDIUMTEXT)
    numbering = db.Column(db.Integer)
    disable_printing = db.Column('disableprinting', db.Boolean)
    custom_titles = db.Column('customtitles', db.Boolean)
    time_created = db.Column('timecreated', db.Integer)
    time_modified = db.Column('timemodified', db.Integer)
    contest_id = db.Column(db.Integer)
    time_start = db.Column('timestart', db.Integer)
    time_stop = db.Column('timestop', db.Integer)
    olympiad = db.Column(db.Boolean)
    virtual_olympiad = db.Column(db.Boolean)
    virtual_duration = db.Column(db.Integer)
    settings = db.Column(JsonType)

    course = db.relationship('Course', backref=db.backref('statements', lazy='dynamic'))

    problems = association_proxy('StatementProblems', 'problem')
    user = association_proxy('StatementUsers1', 'user')

    SETTINGS_SCHEMA = {
        'type': 'object',
        'properties': {
            'allowed_languages': {
                'type': 'array',
                'uniqueItems': True,
                # rmatics lang_ids; the judges (judges.json "langs") define
                # which ones exist, so they aren't enumerated here
                'items': {
                    'type': 'integer',
                    'minimum': 0,
                }
            },
            'type': {
                'oneOf': [
                    {
                        'type': 'null',
                    },
                    {
                        'type': 'string',
                        'enum': [
                            'olympiad',
                            'virtual',
                        ],
                    }
                ],
            },
            'group': {
                'type': 'integer',
            },
            'team': {
                'type': 'boolean',
            },
            'time_start': {
                'type': 'integer',
            },
            'time_stop': {
                'type': 'integer',
            },
            'freeze_time': {
                'type': 'integer',
            },
            'standings': {
                'type': 'boolean',
            },
            'test_only_samples': {
                'type': 'boolean',
            },
            'reset_submits_on_start': {
                'type': 'boolean',
            },
            'test_until_fail': {
                'type': 'boolean',
            },
            'start_from_scratch': {
                'type': 'boolean',
            },
            'restrict_view': {
                'type': 'boolean',
            }
        },
        'additionalProperties': False,
    }
    SETTINGS_SCHEMA_VALIDATOR = Draft4Validator(SETTINGS_SCHEMA)

    def get_allowed_languages(self):
        if not (self.settings and 'allowed_languages' in self.settings):
            return None
        return self.settings['allowed_languages']

    def is_language_allowed(self, lang_id: int) -> bool:
        # an empty list is treated as "not set", not as "nothing allowed"
        allowed = self.get_allowed_languages()
        return not allowed or lang_id in allowed

    def set_settings(self, settings):
        validation_error = next(self.SETTINGS_SCHEMA_VALIDATOR.iter_errors(settings), None)
        if validation_error:
            raise ValueError(validation_error.message)
        self.settings = settings

        if settings.get('time_start'):
            self.time_start = settings['time_start']

        if settings.get('time_stop'):
            self.time_stop = settings['time_stop']

        if 'type' in settings:
            type_ = settings['type']
            if type_ == None:
                self.olympiad = False
                self.virtual_olympiad = False
            elif type_ == 'olympiad':
                self.olympiad = True
                self.virtual_olympiad = False
            else:
                self.olympiad = False
                self.virtual_olympiad = True

        self.time_modified = int(time.time())

    def start_participant(self,
                          user,
                          duration,
                          password=None,
                          ):
        if self.course \
                and self.course.require_password() \
                and password != self.course.password:
            raise ValueError()

        if self.participants.filter(Participant.user_id == user.id).count():
            raise ValueError()

        if user.get_active_participant():
            raise ValueError()

        new_participant = Participant(
            user_id=user.id,
            statement_id=self.id,
            start=int(time.time()),
            duration=duration,
        )
        db.session.add(new_participant)

        return new_participant

    def finish_participant(self, user):
        active_participant = user.get_active_participant()
        if not active_participant or active_participant.statement_id != self.id:
            raise ValueError()

        active_participant.duration = int(time.time() - active_participant.start)
        return active_participant

    def start(self,
              user,
              password=None,
              ):
        if not self.olympiad:
            raise ValueError()

        now = time.time()
        if now < self.time_start:
            raise ValueError()
        if now >= self.time_stop:
            raise ValueError()

        return self.start_participant(
            user=user,
            duration=self.time_stop - int(time.time()),
            password=password,
        )

    def finish(self, user):
        if not self.olympiad:
            raise ValueError()

        return self.finish_participant(user)


    def serialize(self, attributes=None):
        if not attributes:
            attributes = (
                'id',
                'name',
                'olympiad',
                'settings',
                'time_start',
                'time_stop',
                'virtual_olympiad',
                'virtual_duration',
                'course_module_id',
                'course',
                'require_password',
            )
        serialized = attrs_to_dict(self, *attributes)
        if 'course' in attributes and self.course:
            serialized['course'] = self.course.serialize()
        serialized['course_module_id'] = getattr(self.course_module, 'id', None)

        if 'require_password' in attributes:
            if self.course:
                serialized['require_password'] = self.course.require_password()
            else:
                serialized['require_password'] = False

        user = getattr(g, 'user', None)
        if self.olympiad or self.virtual_olympiad:
            if not user:
                return serialized

            try:
                participant = self.participants.filter_by(user_id=user.id).one()
            except NoResultFound:
                return serialized

            serialized['participant'] = participant.serialize()

        serialized['problems'] = {
            rank: {
                'id': statement_problem.problem.id,
                'name': statement_problem.problem.name,
            }
            for rank, statement_problem in self.StatementProblems.items()
            if statement_problem.problem and not statement_problem.hidden
        }
        return serialized


class StatementUser(db.Model):
    __table_args__ = {'schema': 'moodle'}
    __tablename__ = 'mdl_olympiad'

    id = db.Column(db.Integer, primary_key=True)
    statement_id = db.Column('contest_id', db.Integer, db.ForeignKey('moodle.mdl_statements.id'))
    user_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_user.id'))

    # statement = db.relationship('Statement', backref=db.backref('StatementUsers1', lazy='dynamic'), lazy='dynamic')
    # user = db.relationship('EjudgeUser', backref=db.backref('StatementUsers2', lazy='dynamic'), lazy='dynamic')


class StatementProblem(db.Model):
    __table_args__ = {'schema': 'moodle'}
    __tablename__ = 'mdl_statements_problems_correlation'

    id = db.Column(db.Integer, primary_key=True)
    statement_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_statements.id'))
    problem_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_problems.id'))
    rank = db.Column('rank', db.Integer)
    hidden = db.Column('hidden', db.Integer)

    statement = db.relationship('Statement', backref=db.backref('StatementProblems', collection_class=attribute_mapped_collection("rank")))

    # reference to the "Keyword" object
    problem = db.relationship('Problem', backref=db.backref('StatementProblems'))

    def __init__(self, statement_id, problem_id, rank):
        self.statement_id = statement_id
        self.problem_id = problem_id
        self.rank = rank
        self.hidden = 0
