import datetime

from rmatics.model.base import db
from rmatics.model.user import SimpleUser

from sqlalchemy.dialects.mysql import INTEGER

class Comment(db.Model):
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['run_id', 'contest_id'], 
            ['ejudge.runs.run_id', 'ejudge.runs.contest_id']
        ),
        {'schema':'ejudge'}
    )
    __tablename__ = 'mdl_run_comments'
   
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime)
    run_id = db.Column(INTEGER(unsigned=True))
    contest_id = db.Column(INTEGER(unsigned=True))
    user_id = db.Column(db.Integer)
    author_user_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_user.id'))
    lines = db.Column(db.Text)
    comment = db.Column(db.UnicodeText)
    is_read = db.Column(db.Boolean)

    author_user = db.relationship(
        SimpleUser, 
        backref='simpleuser1', 
        uselist=False, 
        lazy=False, 
        primaryjoin=(author_user_id == SimpleUser.id),
    )
    run = db.relationship('EjudgeRun', backref='comment', uselist=False)

    def __init__(self,  run, author, lines='', comment='', date=datetime.datetime.now()):
        self.date = date
        self.run_id = run.run_id
        self.user_id = run.user.id
        self.contest_id = run.contest_id
        self.author_user_id = author.id
        self.lines = lines    
        self.comment = comment    
        self.is_read = False
        
    def __json__(self, request):
        return {
            'date' : str(self.date),
            'id' :  self.id,
            'run_id' : self.run_id,
            'user_id' : self.user_id,
            'contest_id' : self.contest_id,
            'author_user_id' : self.author_user_id,
            'lines' : self.lines,
            'comment' : self.comment,
            'is_read' : self.is_read,
            'problem_id' : self.run.problem.id,
            'problem_name' : self.run.problem.name
        }
    
    @staticmethod
    def get_by(run_id, contest_id):
        try:
            return db.session.query(Comment) \
                .filter(Comment.run.run_id == int(run_id)) \
                .filter(Comment.contest_id == int(contest_id)) \
                .first()            
        except:
            return None       
