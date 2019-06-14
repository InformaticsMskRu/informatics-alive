import io
from unittest import mock

from flask import url_for

from rmatics.model import Run
from rmatics.model.base import db
from rmatics.model.user import SimpleUser
from rmatics.testutils import TestCase


class TestAPIProblemSubmission(TestCase):
    def setUp(self):
        super().setUp()

        self.create_ejudge_problems()
        self.create_problems()
        self.create_statements()
        self.create_statement_problems()
        self.create_course_module_statement()

        self.user1 = SimpleUser(firstname='user1', lastname='user1')
        self.user2 = SimpleUser(firstname='user2', lastname='user2')

        db.session.add_all([self.user1, self.user2])

        db.session.flush()

        db.session.commit()

        self.SOURCE_HASH = 'source-hash'
        self.CONTEXT_SOURCE = 1

    def send_request(self, problem_id: int, **kwargs):
        with mock.patch(
                'rmatics.view.problem.problem.TrustedSubmitApi.check_file_restriction') as check_file_restriction, \
                mock.patch('rmatics.view.problem.problem.Run.generate_source_hash') as generate_source_hash, \
                mock.patch('rmatics.view.problem.problem.Run.update_source') as update_source:
            check_file_restriction.return_value = io.BytesIO(bytes((ascii('f') * 64 * 1024).encode('ascii')))
            generate_source_hash.return_value = self.SOURCE_HASH

            payload = {
                'lang_id': 1,
                'user_id': self.user1.id,
                **kwargs
            }

            route = url_for('problem.trusted_submit', problem_id=problem_id)

            return self.client.post(route, data=payload)

    def test_basic_request_with_context(self):
        statement_id = self.statements[0].id
        is_visible = True

        payload = {
            'statement_id': statement_id,
            'context_source': self.CONTEXT_SOURCE,
            'is_visible': is_visible,
        }

        resp = self.send_request(self.ejudge_problems[1].id, **payload)
        self.assert200(resp)

        data = resp.json.get('data')
        self.assertIsNotNone(data)

        run_id = data.get('run_id')
        self.assertIsNotNone(run_id)

        run = db.session.query(Run).get(run_id)
        self.assertIsNotNone(run)

        self.assertEqual(run.statement_id, statement_id)
        self.assertEqual(run.context_source, self.CONTEXT_SOURCE)
        self.assertEqual(run.is_visible, is_visible)
