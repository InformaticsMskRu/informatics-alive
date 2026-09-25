from rmatics.model.statement import Statement
from rmatics.testutils import TestCase


class TestAllowedLanguages(TestCase):
    def test_not_set_allows_everything(self):
        self.assertTrue(Statement().is_language_allowed(27))
        self.assertTrue(Statement(settings={}).is_language_allowed(27))

    def test_empty_list_allows_everything(self):
        self.assertTrue(Statement(settings={'allowed_languages': []}).is_language_allowed(27))

    def test_restricts_to_the_list(self):
        statement = Statement(settings={'allowed_languages': [3, 27]})
        self.assertTrue(statement.is_language_allowed(27))
        self.assertFalse(statement.is_language_allowed(1))

    def test_schema_accepts_current_lang_ids(self):
        # 71 (Kotlin) is not in the old LANG_NAME_BY_ID catalog
        statement = Statement()
        statement.set_settings({'allowed_languages': [3, 71]})
        self.assertEqual(statement.get_allowed_languages(), [3, 71])

    def test_schema_rejects_non_integer_ids(self):
        with self.assertRaises(ValueError):
            Statement().set_settings({'allowed_languages': ['python']})
