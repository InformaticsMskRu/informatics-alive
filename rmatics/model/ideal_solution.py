import datetime
from sqlalchemy import and_

from rmatics.model.base import db
from rmatics.model.ejudge_run import EjudgeRun
from rmatics.model.user import User

from sqlalchemy.dialects.mysql import INTEGER

LANG = {
    1: 'Pascal',
    2: 'C',
    3: 'C++',
    7: 'Pascal',
    8: 'Pascal',
    9: 'C',
    10: 'C++',
    18: 'Java',
    22: 'PHP',
    23: 'Python',
    24: 'Perl',
    25: 'C#',
    26: 'Ruby',
    27: 'Python',
    28: 'Haskell',
    31: '1С',
    32: '1С',
    0: 'text',
}


def get_run_code(run_id, contest_id):
    run_id = int(run_id)
    tmp_id = int(run_id//32)
    contest_id = int(contest_id)
    
    d3 = tmp_id % 32
    tmp_id = tmp_id // 32
    
    d2 = tmp_id % 32
    tmp_id = tmp_id // 32

    d1 = tmp_id % 32
    tmp_id = tmp_id // 32
    
    if d1 > 9:
        d1 = chr(ord('A') + d1 - 10)
    
    if d2 > 9:
        d2 = chr(ord('A') + d2 - 10)

    if d3 > 9:
        d3 = chr(ord('A') + d3 - 10)

    contest_id = str(contest_id)
    while len(contest_id)<6:
        contest_id = '0' + contest_id

    run_id = str(run_id)
    while len(run_id)<6:
        run_id = '0' + run_id

    path = '/home/judges/' + contest_id + '/var/archive/runs/' + str(d1) + '/' + str(d2) + '/' + str(d3) + '/' + run_id
    codefile = open(path)
    code = codefile.read()
    codefile.close()
    return code


class Ideal(db.Model):
    __table_args__ = {'schema': 'moodle'}
    __tablename__ = 'mdl_ideal_solution'

    id = db.Column(db.Integer, primary_key=True)
    problem_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_problems.id'))
    run_id = db.Column(INTEGER(unsigned=True), db.ForeignKey('ejudge.runs.run_id'))
    contest_id = db.Column(INTEGER(unsigned=True), db.ForeignKey('ejudge.runs.contest_id'))
    lang_id = db.Column(db.Integer)
    lang = db.Column(db.Unicode(100))
    code = db.Column(db.UnicodeText)
    author_id = db.Column(db.Integer, db.ForeignKey('moodle.mdl_user.id'))
    author_name = db.Column(db.Unicode(200))
    comment = db.Column(db.UnicodeText)
    status = db.Column(db.Integer)

    def __init__(self, problem_id, run_id, contest_id, author_id, comment, status=0):
        self.problem_id = problem_id
        self.run_id = run_id
        self.contest_id = contest_id
        run = db.session.query(EjudgeRun) \
            .filter_by(contest_id=contest_id) \
            .filter_by(run_id=run_id) \
            .one()
        user = db.session.query(User) \
            .filter_by(id=author_id) \
            .one()
        self.lang_id = run.lang_id
        self.lang = LANG[self.lang_id]
        self.code = get_run_code(run_id, contest_id)
        self.author_id = author_id
        self.author_name = user.firstname + ' ' + user.lastname
        self.comment = comment
        self.status = status

    def __json__(self, request):
        return {
            'id' :  self.id,
            'problem_id' : self.problem_id,
            'lang' : self.lang,
            'code' : self.code,
            'author_name': self.author_name,
            'author_id': self.author_id,
            'comment': self.comment,
        }
