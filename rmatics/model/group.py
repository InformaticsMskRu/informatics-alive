from rmatics.model.base import db
from rmatics.utils.functions import attrs_to_dict


class Group(db.Model):
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['owner_id'],
            ['moodle.mdl_user.id']
        ),
        {'schema': 'moodle'}
    )
    __tablename__ = 'mdl_ejudge_group'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(100))
    description = db.Column(db.Text)
    owner_id = db.Column(db.Integer)
    visible = db.Column(db.Integer)

    owner = db.relationship('SimpleUser', backref=db.backref('groups', lazy='select'), lazy='joined')

    def serialize(self, attributes=None):
        if not attributes:
            attributes = (
                'name',
                'description',
                'owner_id',
                'visible',
            )
        serialized = attrs_to_dict(self, *attributes)
        return serialized


class UserGroup(db.Model):
    __table_args__ = (
        db.UniqueConstraint('user_id', 'group_id', name='group_id'),
        {'schema': 'moodle'},
    )
    __tablename__ = 'mdl_ejudge_group_users'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_user.id'))
    group_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_ejudge_group.id'))

    user = db.relationship('SimpleUser', backref=db.backref('user_groups', lazy='select'))
    group = db.relationship('Group', backref=db.backref('user_groups', lazy='select'))

    @staticmethod
    def create_if_not_exists(user_id, group_id):
        user_group = db.session.query(UserGroup).filter_by(
            user_id=user_id,
            group_id=group_id
        ).first()
        if user_group:
            return None

        return UserGroup(user_id=user_id, group_id=group_id)
