from marshmallow import Schema, fields, post_load


class UserRunSchema(Schema):
    id = fields.Integer(dump_only=True)
    firstname = fields.String()
    lastname = fields.String()


class ProblemRunSchema(Schema):
    id = fields.Integer(dump_only=True)
    name = fields.String()


class RunSchema(Schema):
    id = fields.Integer(dump_only=True)
    user = fields.Nested(UserRunSchema)
    problem = fields.Nested(ProblemRunSchema)
    ejudge_status = fields.Integer()
    create_time = fields.DateTime()
    lang_id = fields.Integer()
    ejudge_language_id = fields.Integer(attribute='lang_id', dump_only=True)
    ejudge_test_num = fields.Integer()
    ejudge_score = fields.Integer()
    code = fields.String()
    context_source = fields.Integer()

    @post_load
    def load_run_update(self, data: dict):
        run = self.context.get('instance')
        for k, v in data.items():
            setattr(run, k, v)

        return run
