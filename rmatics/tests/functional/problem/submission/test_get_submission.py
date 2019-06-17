from datetime import datetime, timedelta

from flask import url_for

from rmatics.model.base import db
from rmatics.model.group import Group, UserGroup
from rmatics.model.run import Run
from rmatics.model.user import SimpleUser
from rmatics.testutils import TestCase

CONTEXT_SOURCE = 10
CONTEXT_ID = 20


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

        self.run1 = Run(user_id=self.user1.id, problem_id=self.problems[1].id,
                        ejudge_status=0, ejudge_language_id=1, is_visible=True)
        self.run2 = Run(user_id=self.user1.id, problem_id=self.problems[2].id,
                        ejudge_status=0, ejudge_language_id=1, is_visible=True)
        self.run3 = Run(user_id=self.user2.id, problem_id=self.problems[1].id,
                        ejudge_status=2, ejudge_language_id=2, is_visible=True)
        self.run4 = Run(user_id=self.user2.id, problem_id=self.problems[2].id,
                        ejudge_status=2, ejudge_language_id=2, is_visible=True)
        self.run5 = Run(user_id=self.user2.id, problem_id=self.problems[1].id,
                        ejudge_status=2, ejudge_language_id=2)

        self.run4.create_time = datetime.utcnow() - timedelta(days=1)

        # Context tests fixtures
        self.run1.context_id = CONTEXT_ID
        self.run2.context_source = CONTEXT_SOURCE

        db.session.add_all([self.run1, self.run2, self.run3, self.run4, self.run5])

        self.group = Group()
        db.session.add(self.group)
        db.session.flush()

        user_group = UserGroup(user_id=self.user1.id, group_id=self.group.id)
        db.session.add(user_group)

        db.session.commit()

    def send_request(self, problem_id: int, **kwargs):
        route = url_for('problem.problem_submissions', problem_id=problem_id)

        data = {
            'page': 1,
            **kwargs
        }

        response = self.client.get(route, data=data)
        return response

    def test_simple(self):
        resp = self.send_request(self.problems[1].id)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 2)

        self.assertIn('metadata', data)

        self.assertIn('count', data['metadata'])
        self.assertEqual(data['metadata']['count'], 2)

        self.assertIn('page_count', data['metadata'])
        self.assertEqual(data['metadata']['page_count'], 1)

        run0 = data['data'][0]

        # Common fields
        self.assertIn('id', run0)
        self.assertIsNotNone(run0['id'])

        self.assertIn('user', run0)
        self.assertIsNotNone(run0['user'])

        self.assertIn('problem', run0)
        self.assertIsNotNone(run0['problem'])

        self.assertIn('ejudge_status', run0)
        self.assertIsNotNone(run0['ejudge_status'])

        self.assertIn('create_time', run0)
        self.assertIsNotNone(run0['create_time'])

        # User
        user = run0['user']
        self.assertIn('id', user)
        self.assertIsNotNone(user['id'])

        self.assertIn('firstname', user)
        self.assertIsNotNone(user['firstname'])

        self.assertIn('lastname', user)
        self.assertIsNotNone(user['lastname'])

        # Problem
        problem = run0['problem']
        self.assertIn('name', problem)
        self.assertIsNotNone(problem['name'])

    def test_filter_by_user_in_problem(self):
        resp = self.send_request(self.problems[1].id, user_id=self.user1.id)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

    def test_filter_by_lang(self):
        resp = self.send_request(self.problems[1].id, lang_id=self.run1.ejudge_language_id)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

    def test_filter_by_status(self):
        resp = self.send_request(self.problems[1].id, status_id=self.run3.ejudge_status)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

        resp = self.send_request(self.problems[1].id, status_id=self.run1.ejudge_status)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

        resp = self.send_request(self.problems[1].id, status_id=-1)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 2)

    def test_filter_by_statement(self):
        resp = self.send_request(self.problems[1].id, statement_id=self.run1.statement_id)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 2)

    def test_filter_by_group(self):
        resp = self.send_request(self.problems[2].id, group_id=self.group.id)
        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

    def test_filter_by_from_timestamp(self):
        from_time = int((datetime.utcnow() - timedelta(hours=1)).timestamp() * 1000)

        resp = self.send_request(self.problems[2].id, from_timestamp=from_time)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

        # Too mush for timestamp
        resp = self.send_request(self.problems[2].id, from_timestamp=from_time * 10000)
        self.assert400(resp)

    def test_filter_by_to_timestamp(self):
        to_time = int((datetime.utcnow() - timedelta(hours=1)).timestamp() * 1000)

        resp = self.send_request(self.problems[2].id, to_timestamp=to_time)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

        # Too mush for timestamp
        resp = self.send_request(self.problems[2].id, to_timestamp=to_time * 10000)
        self.assert400(resp)

    def test_filter_by_course_module(self):
        resp = self.send_request(0, statement_id=self.course_module_statement.id)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 4)

    def test_filter_by_user(self):
        payload = {
            'statement_id': 0,
            'user_id': self.user1.id
        }
        resp = self.send_request(0, **payload)

        self.assert200(resp)
        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 2, 'If statement is 0 and problem is 0 then it is users filter')

    def test_filter_by_zero_status(self):
        resp = self.send_request(self.problems[1].id, status_id=self.run3.ejudge_status)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

    def test_filter_by_context(self):
        resp = self.send_request(self.problems[1].id, context_id=CONTEXT_ID)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

    def test_filter_by_context_source(self):
        resp = self.send_request(self.problems[2].id, context_source=CONTEXT_SOURCE)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 1)

    def test_filter_by_visibillity(self):
        resp = self.send_request(self.problems[1].id, show_hidden=True)

        self.assert200(resp)

        data = resp.get_json()
        self.assertEqual(data['result'], 'success')
        self.assertEqual(len(data['data']), 3)
