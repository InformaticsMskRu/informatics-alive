import json
import os
import tempfile

from rmatics.ejudge.judges_config import JudgeConfig, JudgeLang, _load, get_default_judge_id
from rmatics.testutils import TestCase


class TestJudgesConfigLangs(TestCase):
    def _load(self, judge_cfg):
        cfg = {'url': 'http://ejudge', 'token': 't', **judge_cfg}
        fd, path = tempfile.mkstemp(suffix='.json')
        self.addCleanup(os.remove, path)
        with os.fdopen(fd, 'w') as f:
            json.dump({'5': cfg}, f)
        return _load(path)[5]

    def test_langs_absent_accepts_no_languages(self):
        with self.assertLogs('rmatics.ejudge.judges_config', level='ERROR'):
            judge = self._load({})
        self.assertEqual(judge.langs, {})
        self.assertFalse(judge.supports_lang(27))

    def test_langs_parsed(self):
        judge = self._load({'langs': {
            '27': {'name': ' Python 3.9 ', 'ejudge_lang_id': 62},
            '3': {'name': 'GNU C++ 11.2', 'ejudge_lang_id': 3},
        }})
        self.assertEqual(judge.langs, {27: JudgeLang('Python 3.9', 62),
                                       3: JudgeLang('GNU C++ 11.2', 3)})

    def test_invalid_entries_are_skipped(self):
        judge = self._load({'langs': {
            'x': {'name': 'Bad id', 'ejudge_lang_id': 1},
            '1': {'name': '', 'ejudge_lang_id': 1},
            '2': {'name': 'No ejudge id'},
            '4': {'name': 'Bool id', 'ejudge_lang_id': True},
            '5': 'Name only',
            '3': {'name': 'C++', 'ejudge_lang_id': 3},
        }})
        self.assertEqual(judge.langs, {3: JudgeLang('C++', 3)})

    def test_lang_map_key_is_ignored(self):
        with self.assertLogs('rmatics.ejudge.judges_config', level='WARNING') as logs:
            judge = self._load({'lang_map': {'27': 62}, 'langs': {
                '27': {'name': 'Python 3.9', 'ejudge_lang_id': 64}}})
        self.assertIn('"lang_map" is not supported', logs.output[0])
        self.assertEqual(judge.map_lang_id(27), 64)

    def test_langs_not_an_object_accepts_no_languages(self):
        with self.assertLogs('rmatics.ejudge.judges_config', level='ERROR'):
            self.assertEqual(self._load({'langs': ['Python 3.9']}).langs, {})

    def test_supports_lang(self):
        self.assertFalse(JudgeConfig(url='u').supports_lang(27))
        judge = JudgeConfig(url='u', langs={3: JudgeLang('C++', 3)})
        self.assertTrue(judge.supports_lang(3))
        self.assertFalse(judge.supports_lang(27))


class TestMapLangId(TestCase):
    def test_langs_mapping(self):
        judge = JudgeConfig(url='u', langs={27: JudgeLang('Python 3.9', 62)})
        self.assertEqual(judge.map_lang_id(27), 62)

    def test_lang_outside_langs_raises(self):
        judge = JudgeConfig(url='u', langs={27: JudgeLang('Python 3.9', 62)})
        with self.assertRaises(ValueError):
            judge.map_lang_id(3)




class TestDefaultJudgeId(TestCase):
    def test_int_value(self):
        self.app.config['DEFAULT_JUDGE_ID'] = '2'
        self.assertEqual(get_default_judge_id(), 2)

    def test_empty_or_invalid_value(self):
        for value in (None, '', 'abc'):
            self.app.config['DEFAULT_JUDGE_ID'] = value
            self.assertIsNone(get_default_judge_id())
