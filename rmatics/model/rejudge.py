from rmatics.model.base import db


class Rejudge(db.Model):
    __table_args__ = {'schema': 'pynformatics'}
    __tablename__ = 'rejudge'
    # TODO: добавить старый ejudge_run_id
    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.Integer, db.ForeignKey('pynformatics.runs.id'), nullable=False)
    ejudge_contest_id = db.Column(db.Integer, nullable=False)
    ejudge_url = db.Column(db.String(50))
