from flask import url_for

from rmatics.model.base import db
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
        route = url_for('problem.problem_languages', problem_id=problem_id or self.problem_id)
        return self.client.get(route, query_string=params)

    def test_lists_languages(self):
        response = self.get(user_id=1)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['data'], [
            {'id': 1, 'name': 'Free Pascal 3.0'},
            {'id': 3, 'name': 'GNU C++ 11.2'},
            {'id': 27, 'name': 'Python 3.9'},
        ])

    def test_statement_restricts_languages(self):
        statement = self.statements[0]
        statement.settings = {'allowed_languages': [27]}
        db.session.commit()
        response = self.get(user_id=1, statement_id=statement.id)
        self.assertEqual([lang['id'] for lang in response.json['data']], [27])

    def test_context_id_wins_over_statement_id(self):
        self.statements[1].settings = {'allowed_languages': [3]}
        db.session.commit()
        response = self.get(user_id=1, statement_id=self.statements[0].id,
                            context_id=self.statements[1].id)
        self.assertEqual([lang['id'] for lang in response.json['data']], [3])

    def test_unknown_problem(self):
        self.assertEqual(self.get(problem_id=99999, user_id=1).status_code, 404)

    def test_user_id_is_required(self):
        self.assertEqual(self.get().status_code, 422)

    def test_output_only_language_has_no_name(self):
        self.ejudge_problems[0].output_only = True
        db.session.commit()
        self.assertEqual(self.get(user_id=1).json['data'], [{'id': 0, 'name': None}])
