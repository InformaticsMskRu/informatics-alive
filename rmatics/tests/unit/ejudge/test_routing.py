from unittest import mock

from rmatics.ejudge.judges_config import JudgeLang
from rmatics.ejudge.routing import Route, available_languages, resolve_route
from rmatics.model.statement import Statement
from rmatics.utils.exceptions import LanguageNotSupported
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

    def test_no_settings_checks_default_judge_langs(self):
        self.judges[1].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        self.assertEqual(resolve_route(_problem(), 3, USER), Route(1, 100, 3))
        with self.assertRaises(LanguageNotSupported):
            resolve_route(_problem(), 27, USER)

    def test_no_settings_output_only_skips_default_judge_langs(self):
        self.judges[1].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        self.assertEqual(resolve_route(_problem(output_only=True), 0, USER), Route(1, 100, 3))

    def test_entry_route(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6}])
        self.assertEqual(resolve_route(problem, 27, USER), Route(2, 500, 6))

    def test_no_matching_entry_is_rejected(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'lang_ids': [3]}])
        with self.assertRaises(LanguageNotSupported):
            resolve_route(problem, 27, USER)

    def test_judge_without_the_language_is_rejected(self):
        self.judges[2].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6}])
        with self.assertRaises(LanguageNotSupported):
            resolve_route(problem, 27, USER)
        self.assertEqual(resolve_route(problem, 3, USER), Route(2, 500, 6))

    def test_falls_through_to_an_entry_whose_judge_supports_the_language(self):
        self.judges[2].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        self.judges[1].langs = {71: JudgeLang('Kotlin 1.9', 71)}
        problem = _problem([
            {'judge_id': 2, 'contest_id': 500, 'problem_id': 6},
            {'judge_id': 1, 'contest_id': 700, 'problem_id': 1},
        ])
        self.assertEqual(resolve_route(problem, 3, USER), Route(2, 500, 6))
        self.assertEqual(resolve_route(problem, 71, USER), Route(1, 700, 1))
        with self.assertRaises(LanguageNotSupported):
            resolve_route(problem, 27, USER)

    def test_lang_ids_narrow_the_judge_languages(self):
        self.judges[2].langs = {3: JudgeLang('GNU C++ 11.2', 3),
                                27: JudgeLang('Python 3.9', 62)}
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'lang_ids': [3]}])
        self.assertEqual(resolve_route(problem, 3, USER), Route(2, 500, 6))
        with self.assertRaises(LanguageNotSupported):
            resolve_route(problem, 27, USER)

    def test_lang_ids_cannot_add_a_language_to_the_judge(self):
        """lang_ids lists a language the judge lacks: the more specific entry
        is passed over (with a warning) for one whose judge supports it."""
        self.judges[2].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        self.judges[1].langs = {27: JudgeLang('Python 3.9', 27)}
        problem = _problem([
            {'judge_id': 2, 'contest_id': 500, 'problem_id': 6, 'lang_ids': [27]},
            {'judge_id': 1, 'contest_id': 700, 'problem_id': 1},
        ])
        with self.assertLogs('rmatics.ejudge.routing', level='WARNING') as logs:
            self.assertEqual(resolve_route(problem, 27, USER), Route(1, 700, 1))
        self.assertIn('lang_ids lists lang_id 27', logs.output[0])

    def test_entry_without_judge_id_is_rejected(self):
        problem = _problem([{'contest_id': 500, 'problem_id': 6}])
        with self.assertRaises(LanguageNotSupported):
            resolve_route(problem, 27, USER)

    def test_string_judge_id(self):
        self.judges[2].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        problem = _problem([{'judge_id': '2', 'contest_id': 500, 'problem_id': 6}])
        self.assertEqual(resolve_route(problem, 3, USER), Route(2, 500, 6))
        with self.assertRaises(LanguageNotSupported):
            resolve_route(problem, 27, USER)

    def test_output_only_skips_language_check(self):
        self.judges[2].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6}],
                           output_only=True)
        self.assertEqual(resolve_route(problem, 0, USER), Route(2, 500, 6))

    def test_output_only_ignores_lang_ids(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'lang_ids': [3]}], output_only=True)
        self.assertEqual(resolve_route(problem, 0, USER), Route(2, 500, 6))

    def test_output_only_lang_ids_add_no_specificity(self):
        # without lang_ids counting, both entries are equally specific:
        # the first listed wins
        problem = _problem([
            {'judge_id': 1, 'contest_id': 700, 'problem_id': 1},
            {'judge_id': 2, 'contest_id': 500, 'problem_id': 6, 'lang_ids': [0]},
        ], output_only=True)
        self.assertEqual(resolve_route(problem, 0, USER), Route(1, 700, 1))

    def test_output_only_keeps_user_ids(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'user_ids': [USER]}], output_only=True)
        self.assertEqual(resolve_route(problem, 0, USER), Route(2, 500, 6))
        with self.assertRaises(LanguageNotSupported):
            resolve_route(problem, 0, OTHER_USER)

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
        with self.assertRaises(LanguageNotSupported):
            resolve_route(problem, 27, OTHER_USER)


class TestAvailableLanguages(TestCase):
    def setUp(self):
        super().setUp()
        self.create_judges()

    def ids(self, problem, statement=None):
        return [lang['id'] for lang in available_languages(problem, USER, statement)]

    def test_no_settings_lists_default_judge_langs_with_names(self):
        self.assertEqual(available_languages(_problem(), USER), [
            {'id': 1, 'name': 'Free Pascal 3.0'},
            {'id': 3, 'name': 'GNU C++ 11.2'},
            {'id': 27, 'name': 'Python 3.9'},
        ])

    def test_no_settings_ignores_langs_only_other_judges_have(self):
        self.judges[1].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        self.assertEqual(self.ids(_problem()), [3])

    def test_languages_come_from_the_routed_judge(self):
        self.judges[2].langs = {71: JudgeLang('Kotlin 1.9', 71)}
        self.judges[1].langs = {3: JudgeLang('GNU C++ 11.2', 3)}
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6}])
        self.assertEqual(available_languages(problem, USER),
                         [{'id': 71, 'name': 'Kotlin 1.9'}])

    def test_lang_ids_narrow_the_judge_langs(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'lang_ids': [3, 27]}])
        self.assertEqual(self.ids(problem), [3, 27])

    def test_user_ids_restrict_the_entry(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 500, 'problem_id': 6,
                             'user_ids': [OTHER_USER]}])
        self.assertEqual(self.ids(problem), [])

    def test_entry_with_unknown_judge_is_left_out(self):
        problem = _problem([{'judge_id': 99, 'contest_id': 500, 'problem_id': 6}])
        self.assertEqual(self.ids(problem), [])

    def test_no_default_judge_in_config_lists_nothing(self):
        self.app.config['DEFAULT_JUDGE_ID'] = 99
        self.assertEqual(self.ids(_problem()), [])

    def test_statement_allowed_languages_intersect(self):
        statement = Statement(settings={'allowed_languages': [3, 71]})
        self.assertEqual(self.ids(_problem(), statement), [3])

    def test_statement_without_allowed_languages_keeps_all(self):
        self.assertEqual(self.ids(_problem(), Statement(settings={'allowed_languages': []})),
                         [1, 3, 27])

    def test_output_only(self):
        problem = _problem(output_only=True)
        statement = Statement(settings={'allowed_languages': [27]})
        self.assertEqual(available_languages(problem, USER, statement),
                         [{'id': 0, 'name': 'Текстовый файл'}])

    def test_output_only_unroutable(self):
        problem = _problem([{'judge_id': 2, 'contest_id': 5, 'problem_id': 6,
                             'user_ids': [OTHER_USER]}], output_only=True)
        self.assertEqual(self.ids(problem), [])
