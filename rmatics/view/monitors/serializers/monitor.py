from marshmallow import fields, Schema

from rmatics.view.problem.serializers.run import UserRunSchema


class ProblemSchema(Schema):
    id = fields.Integer(dump_only=True)
    name = fields.String(dump_only=True)
    rank = fields.Integer(dump_only=True)


class RunSchema(Schema):
    id = fields.Integer(dump_only=True)
    user = fields.Nested(UserRunSchema, dump_only=True)
    create_time = fields.DateTime()
    ejudge_score = fields.Integer(dump_only=True)
    ejudge_status = fields.Integer(dump_only=True)
    ejudge_test_num = fields.Integer(dump_only=True)


class ContestBasedMonitorSchema(Schema):
    contest_id = fields.Integer(dump_only=True)
    problem = fields.Nested(ProblemSchema, dump_only=True)
    runs = fields.List(fields.Dict(), dump_only=True)


class ProblemBasedMonitorSchema(Schema):
    problem_id = fields.Integer(dump_only=True)
    runs = fields.List(fields.Dict(), dump_only=True)
