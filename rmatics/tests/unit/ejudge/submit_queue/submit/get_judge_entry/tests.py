from unittest import mock

from rmatics.ejudge.submit_queue.submit import _get_judge_entry
from rmatics.testutils import TestCase


def _problem(settings):
    return mock.Mock(judges_settings=settings, id=1)


def _call(settings, lang_id=1, user_id=1):
    return _get_judge_entry(_problem(settings), lang_id, user_id)


class TestGetJudgeEntry(TestCase):

    # --- null / empty settings ---

    def test_none_settings_returns_none(self):
        self.assertIsNone(_call(None))

    def test_empty_settings_returns_none(self):
        self.assertIsNone(_call([]))

    # --- required-key validation ---

    def test_entry_missing_problem_id_is_skipped(self):
        result = _call([
            {'contest_id': 1, 'lang_ids': [1]},                          # missing problem_id
            {'contest_id': 2, 'problem_id': 9, 'lang_ids': [1]},         # valid
        ])
        self.assertEqual(result['contest_id'], 2)

    def test_entry_missing_contest_id_is_skipped(self):
        result = _call([
            {'problem_id': 1, 'lang_ids': [1]},                          # missing contest_id
            {'contest_id': 2, 'problem_id': 9, 'lang_ids': [1]},         # valid
        ])
        self.assertEqual(result['contest_id'], 2)

    def test_all_invalid_entries_returns_none(self):
        self.assertIsNone(_call([{'judge_id': 'x', 'lang_ids': [1]}]))

    # --- lang_ids matching ---

    def test_lang_id_in_lang_ids_matches(self):
        self.assertIsNotNone(_call(
            [{'contest_id': 1, 'problem_id': 1, 'lang_ids': [27]}],
            lang_id=27,
        ))

    def test_lang_id_not_in_lang_ids_no_match(self):
        self.assertIsNone(_call(
            [{'contest_id': 1, 'problem_id': 1, 'lang_ids': [27]}],
            lang_id=1,
        ))

    def test_null_lang_ids_matches_any_lang(self):
        self.assertIsNotNone(_call(
            [{'contest_id': 1, 'problem_id': 1}],
            lang_id=99,
        ))

    # --- user_ids matching ---

    def test_user_id_in_user_ids_matches(self):
        self.assertIsNotNone(_call(
            [{'contest_id': 1, 'problem_id': 1, 'user_ids': [42]}],
            user_id=42,
        ))

    def test_user_id_not_in_user_ids_no_match(self):
        self.assertIsNone(_call(
            [{'contest_id': 1, 'problem_id': 1, 'user_ids': [42]}],
            user_id=1,
        ))

    def test_null_user_ids_matches_any_user(self):
        self.assertIsNotNone(_call(
            [{'contest_id': 1, 'problem_id': 1}],
            user_id=99,
        ))

    # --- both filters must match ---

    def test_lang_matches_but_user_does_not(self):
        self.assertIsNone(_call(
            [{'contest_id': 1, 'problem_id': 1, 'lang_ids': [1], 'user_ids': [99]}],
            lang_id=1, user_id=1,
        ))

    def test_user_matches_but_lang_does_not(self):
        self.assertIsNone(_call(
            [{'contest_id': 1, 'problem_id': 1, 'lang_ids': [99], 'user_ids': [1]}],
            lang_id=1, user_id=1,
        ))

    # --- specificity ordering ---

    def test_both_filters_beats_lang_only(self):
        result = _call([
            {'contest_id': 1, 'problem_id': 1, 'lang_ids': [1]},
            {'contest_id': 2, 'problem_id': 2, 'lang_ids': [1], 'user_ids': [1]},
        ], lang_id=1, user_id=1)
        self.assertEqual(result['contest_id'], 2)

    def test_both_filters_beats_user_only(self):
        result = _call([
            {'contest_id': 1, 'problem_id': 1, 'user_ids': [1]},
            {'contest_id': 2, 'problem_id': 2, 'lang_ids': [1], 'user_ids': [1]},
        ], lang_id=1, user_id=1)
        self.assertEqual(result['contest_id'], 2)

    def test_lang_only_beats_default(self):
        result = _call([
            {'contest_id': 1, 'problem_id': 1},
            {'contest_id': 2, 'problem_id': 2, 'lang_ids': [1]},
        ], lang_id=1)
        self.assertEqual(result['contest_id'], 2)

    def test_user_only_beats_default(self):
        result = _call([
            {'contest_id': 1, 'problem_id': 1},
            {'contest_id': 2, 'problem_id': 2, 'user_ids': [1]},
        ], user_id=1)
        self.assertEqual(result['contest_id'], 2)

    def test_listed_order_breaks_ties(self):
        result = _call([
            {'contest_id': 1, 'problem_id': 1, 'lang_ids': [1]},
            {'contest_id': 2, 'problem_id': 2, 'lang_ids': [1]},
        ], lang_id=1)
        self.assertEqual(result['contest_id'], 1)

    def test_default_entry_returned_when_nothing_else_matches(self):
        result = _call([
            {'contest_id': 1, 'problem_id': 1, 'lang_ids': [99]},
            {'contest_id': 2, 'problem_id': 2},
        ], lang_id=1)
        self.assertEqual(result['contest_id'], 2)

    def test_no_match_and_no_default_returns_none(self):
        self.assertIsNone(_call(
            [{'contest_id': 1, 'problem_id': 1, 'lang_ids': [99]}],
            lang_id=1,
        ))
