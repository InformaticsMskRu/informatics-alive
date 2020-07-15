from typing import Callable

from sqlalchemy import and_, or_, MetaData, Table
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Query

from rmatics.model.base import db
from rmatics.model.statement import StatementUser
from rmatics.utils.functions import (
    hash_password,
    random_password,
)


def lazy(func):
    """
    A decorator function designed to wrap attributes that need to be
    generated, but will not change. This is useful if the attribute is  
    used a lot, but also often never used, as it gives us speed in both
    situations.
    """
    def cached(self, *args):
        name = "_" + func.__name__
        try:
            return getattr(self, name)
        except AttributeError:
            value = func(self, *args)
            setattr(self, name, value)
            return value
    return cached


class SimpleUser(db.Model):
    RESET_PASSWORD_LENGTH = 20

    __table_args__ = (
        db.Index('ej_id', 'ej_id'),
        {'schema': 'moodle'}
    )
    __tablename__ = 'mdl_user'

    id = db.Column(db.Integer, primary_key=True)
    firstname = db.Column(db.Unicode(100))
    lastname = db.Column(db.Unicode(100))
    login = db.Column('ej_login', db.Unicode(50))
    password = db.Column('ej_password', db.Unicode(50))
    deleted = db.Column('deleted', db.Boolean)
    ejudge_id = db.Column('ej_id', db.Integer)
    problems_solved = db.Column(db.Integer)
    password_md5 = db.Column('password', db.Unicode(32))

    statement = db.relationship(
        'Statement', 
        secondary=StatementUser.__table__,
        backref='StatementUsers1', 
        lazy='dynamic',
    )
    statements = association_proxy('StatementUsers2', 'statement')

    def reset_password(self):
        """
        Генерирует случайный пароль для пользователя и возвращает его
        """
        new_password = random_password(self.RESET_PASSWORD_LENGTH)
        self.password_md5 = hash_password(new_password)
        return new_password


class User(SimpleUser):
    __mapper_args__ = {'polymorphic_identity': 'user'}
    username = db.Column(db.Unicode(100))
    email = db.Column(db.Unicode(100))
    city = db.Column(db.Unicode(20))
    school = db.Column(db.Unicode(255))
    problems_week_solved = db.Column(db.Integer)

    @classmethod
    def search(cls, filter_func: Callable[[Query], Query], filter_deleted=True):
        if filter_deleted:
            users_query = filter_func(db.session.query(cls).filter(cls.deleted == False))
        else:
            users_query = filter_func(db.session.query(cls))
        return users_query

    @classmethod
    def search_by_string(cls, search_string):
        def filter_func(query: Query):
            if search_string.count(' '):
                str1, str2 = search_string.split(' ', 1)
                query = query.filter(or_(
                    and_(cls.firstname.like("%{}%".format(str1)), cls.lastname.like("%{}%".format(str2))),
                    and_(cls.lastname.like("%{}%".format(str1)), cls.firstname.like("%{}%".format(str2))),
                ))
            else:
                query = query.filter(or_(
                    cls.email.like("%{}%".format(search_string)),
                    cls.username.like("%{}%".format(search_string)),
                    cls.firstname.like("%{}%".format(search_string)),
                    cls.lastname.like("%{}%".format(search_string)),
                ))
            return query

        return cls.search(filter_func)

    @lazy
    def _get_current_olymp(self): 
        return None


class PynformaticsUser(User):
    __table_args__ = {'schema': 'moodle'}
    __tablename__ = 'mdl_user_settings'
    __mapper_args__ = {'polymorphic_identity': 'pynformaticsuser'}
    
    id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_user.id'), primary_key=True)
    main_page_settings = db.Column(db.Text)


# SQLAlchemy Core table to avoiding class instance creation
LightWeightUser = Table(
    'mdl_user', MetaData(),
    db.Column('id', db.Integer, primary_key=True),
    db.Column('firstname', db.Unicode(100)),
    db.Column('lastname', db.Unicode(100)),
    db.Column('email', db.Unicode(100)),
    db.Column('username', db.Unicode(100)),
    schema='moodle'
)
