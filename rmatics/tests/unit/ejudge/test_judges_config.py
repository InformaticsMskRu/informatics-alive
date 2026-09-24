import json
import os
import tempfile

from rmatics.ejudge.judges_config import JudgeConfig, _load, get_default_judge_id
from rmatics.testutils import TestCase


class TestJudgesConfigLangs(TestCase):
    def _load(self, judge_cfg):
        cfg = {'url': 'http://ejudge', 'token': 't', **judge_cfg}
        fd, path = tempfile.mkstemp(suffix='.json')
        self.addCleanup(os.remove, path)
        with os.fdopen(fd, 'w') as f:
            json.dump({'5': cfg}, f)
        return _load(path)[5]

    def test_langs_absent(self):
        self.assertIsNone(self._load({}).langs)

    def test_langs_parsed(self):
        judge = self._load({'langs': {'27': ' Python 3.9 ', '3': 'GNU C++ 11.2'}})
        self.assertEqual(judge.langs, {27: 'Python 3.9', 3: 'GNU C++ 11.2'})

    def test_invalid_entries_are_skipped(self):
        judge = self._load({'langs': {'x': 'Bad id', '1': '', '2': None, '3': 'C++'}})
        self.assertEqual(judge.langs, {3: 'C++'})

    def test_langs_not_an_object_is_ignored(self):
        self.assertIsNone(self._load({'langs': ['Python 3.9']}).langs)

    def test_supports_lang(self):
        self.assertTrue(JudgeConfig(url='u').supports_lang(27))
        judge = JudgeConfig(url='u', langs={3: 'C++'})
        self.assertTrue(judge.supports_lang(3))
        self.assertFalse(judge.supports_lang(27))


class TestDefaultJudgeId(TestCase):
    def test_int_value(self):
        self.app.config['DEFAULT_JUDGE_ID'] = '2'
        self.assertEqual(get_default_judge_id(), 2)

    def test_empty_or_invalid_value(self):
        for value in (None, '', 'abc'):
            self.app.config['DEFAULT_JUDGE_ID'] = value
            self.assertIsNone(get_default_judge_id())
