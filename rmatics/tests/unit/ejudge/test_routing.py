from unittest import mock

from rmatics.ejudge.routing import LanguageNotAvailable, Route, resolve_route
from rmatics.testutils import TestCase

USER = 1
OTHER_USER = 2


def _problem(settings=None, output_only=False):
    return mock.Mock(judges_settings=settings, id=10, ejudge_contest_id=100,
                     problem_id=3, output_only=output_only)


class TestResolveRoute(TestCase):
    def setUp(self):
        super().setUp()
        self.create_judges()  # judge 1 is the default one

    def test_no_settings_goes_to_default_judge(self):
        self.assertEqual(resolve_route(_problem(), 27, USER), Route(1, 100, 3))

    def test_no_settings_ignores_default_judge_langs(self):
        self.judges[1].langs = {3: 'GNU C++ 11.2'}
        self.assertEqual(resolve_route(_problem(), 27, USER), Route(1, 100, 3))

    def test_entry_route(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6}])
        self.assertEqual(resolve_route(problem, 27, USER), Route(2, 500, 6))

    def test_no_matching_entry_is_rejected(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'lang_ids': [3]}])
        with self.assertRaises(LanguageNotAvailable):
            resolve_route(problem, 27, USER)

    def test_judge_without_the_language_is_rejected(self):
        self.judges[2].langs = {3: 'GNU C++ 11.2'}
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6}])
        with self.assertRaises(LanguageNotAvailable):
            resolve_route(problem, 27, USER)
        self.assertEqual(resolve_route(problem, 3, USER), Route(2, 500, 6))

    def test_entry_without_judge_id_is_rejected(self):
        problem = _problem([{'contest_id': 500, 'problem_id': 6}])
        with self.assertRaises(LanguageNotAvailable):
            resolve_route(problem, 27, USER)

    def test_string_judge_id(self):
        self.judges[2].langs = {3: 'GNU C++ 11.2'}
        problem = _problem([{'judge_id': '2', 'contest_id': 500, 'problem_id': 6}])
        self.assertEqual(resolve_route(problem, 3, USER), Route(2, 500, 6))
        with self.assertRaises(LanguageNotAvailable):
            resolve_route(problem, 27, USER)

    def test_output_only_skips_language_check(self):
        self.judges[2].langs = {3: 'GNU C++ 11.2'}
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6}],
                           output_only=True)
        self.assertEqual(resolve_route(problem, 0, USER), Route(2, 500, 6))

    def test_unknown_judge_is_left_to_the_caller(self):
        problem = _problem([{'judge_id': 99, 'contest_id': 500, 'problem_id': 6}])
        self.assertEqual(resolve_route(problem, 27, USER), Route(99, 500, 6))

    def test_user_ids_entry_routes_listed_user(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'user_ids': [USER]}])
        self.assertEqual(resolve_route(problem, 27, USER), Route(2, 500, 6))

    def test_user_ids_entry_for_another_user_is_rejected(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'user_ids': [USER]}])
        with self.assertRaises(LanguageNotAvailable):
            resolve_route(problem, 27, OTHER_USER)
