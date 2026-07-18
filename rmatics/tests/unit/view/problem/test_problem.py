from flask import url_for

from rmatics.testutils import TestCase


class TestProblem(TestCase):
    def setUp(self):
        super().setUp()
        self.create_ejudge_problems()

    def send_request(self, problem_id, **kwargs):
        url = url_for('problem.problem', problem_id=problem_id)
        response = self.client.get(url, **kwargs)
        return response

    def test_simple(self):
        resp = self.send_request(self.ejudge_problems[0].id)

        self.assert200(resp)
        self.assertIn('status', resp.json)
        self.assertEqual('success', resp.json['status'])
        self.assertIn('data', resp.json)

        expected_keys = [
             'content',
             'id',
             'memorylimit',
             'name',
             'output_only',
             'show_limits',
             'timelimit'
        ]

        for key in expected_keys:
            self.assertIn(key, resp.json['data'])

    def test_not_found(self):
        resp = self.send_request(6767676)
        self.assert404(resp)
