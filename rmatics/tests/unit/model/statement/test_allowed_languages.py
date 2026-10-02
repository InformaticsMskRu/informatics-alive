from rmatics.model.statement import Statement
from rmatics.testutils import TestCase


class TestAllowedLanguagesSettings(TestCase):
    def test_schema_accepts_current_lang_ids(self):
        # 71 (Kotlin) is not in the old LANG_NAME_BY_ID catalog
        statement = Statement()
        statement.set_settings({'allowed_languages': [3, 71]})
        self.assertEqual(statement.settings['allowed_languages'], [3, 71])

    def test_schema_rejects_non_integer_ids(self):
        with self.assertRaises(ValueError):
            Statement().set_settings({'allowed_languages': ['python']})
