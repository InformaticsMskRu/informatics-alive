from unittest import mock

from flask import url_for

from rmatics.model.base import db
from rmatics.model.problem import EjudgeProblem
from rmatics.testutils import TestCase


class TestProblemLanguages(TestCase):
    def setUp(self):
        super().setUp()
        self.create_judges()
        self.create_ejudge_problems()
        self.create_statements()
        db.session.commit()
        self.problem_id = self.ejudge_problems[0].id

    def get(self, problem_id=None, **params):
        route = url_for('problem.problem', problem_id=problem_id or self.problem_id)
        return self.client.get(route, query_string=params)

    def test_lists_languages(self):
        response = self.get(user_id=1)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['data']['languages'], [
            {'id': 1, 'name': 'Free Pascal 3.0'},
            {'id': 3, 'name': 'GNU C++ 11.2'},
            {'id': 27, 'name': 'Python 3.9'},
        ])
        self.assertEqual(response.json['data']['id'], self.problem_id)

    def test_languages_only_with_user_id(self):
        response = self.get()
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('languages', response.json['data'])

    def test_statement_allowed_languages_are_not_applied(self):
        statement = self.statements[0]
        statement.settings = {'allowed_languages': [27]}
        db.session.commit()
        response = self.get(user_id=1, statement_id=statement.id)
        self.assertEqual([lang['id'] for lang in response.json['data']['languages']],
                         [1, 3, 27])

    def test_output_only_language_has_no_name(self):
        self.ejudge_problems[0].output_only = True
        db.session.commit()
        self.assertEqual(self.get(user_id=1).json['data']['languages'],
                         [{'id': 0, 'name': None}])

    def test_exclude_leaves_fields_out(self):
        response = self.get(exclude='content,sample_tests_json')
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('content', response.json['data'])
        self.assertNotIn('sample_tests_json', response.json['data'])
        self.assertIn('name', response.json['data'])

    def test_exclude_does_not_hide_languages(self):
        response = self.get(user_id=1, exclude='sample_tests_json')
        self.assertIn('languages', response.json['data'])

    def test_exclude_unknown_field_is_rejected(self):
        self.assertEqual(self.get(exclude='nope').status_code, 400)

    def test_exclude_skips_the_samples_from_disk(self):
        self.ejudge_problems[0].sample_tests = '1'
        db.session.commit()
        with mock.patch.object(EjudgeProblem, 'generateSamplesJson') as generate:
            self.get(user_id=1, exclude='sample_tests_json')
            generate.assert_not_called()
            self.get(user_id=1)
            generate.assert_called_once()

    def test_unknown_problem(self):
        self.assertEqual(self.get(problem_id=99999, user_id=1).status_code, 404)
