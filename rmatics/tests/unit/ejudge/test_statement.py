import unittest
from unittest import mock

from rmatics.ejudge import statement
from rmatics.ejudge.import_log import ImportLog
from rmatics.ejudge.judges_config import JudgeConfig

JUDGE = JudgeConfig(url='http://ejudge-2/cgi-bin/new-master', token='token-2')
ERROR_PAGE = ('text/html', b'<html><body>Operation failed</body></html>')
JSON_ERROR = ('application/json', b'{"ok":false,"error":{"symbol":"ERR_INV_FILE_NAME","message":"Invalid file name"}}')


class TestFetchStatement(unittest.TestCase):
    def fetch(self, files):
        def get_file(judge, contest_id, prob_id, name):
            return files.get(name, ERROR_PAGE)

        with mock.patch.object(statement.ejudge_api, 'get_file', side_effect=get_file) as get:
            self.log = ImportLog(2, 2395)
            return (statement.fetch_statement(JUDGE, 2395, 3, self.log),
                    [c[0][3] for c in get.call_args_list])

    def page(self, body):
        return ('text/html', f'<html><body><div class="problem-statement">{body}</div></body></html>'.encode())

    def test_missing_statement(self):
        result, fetched = self.fetch({})

        self.assertIsNone(result)
        self.assertEqual(fetched, ['problem.html'])
        # what get-file replied instead
        self.assertIn('no statement, get-file problem.html replied text/html (42 bytes)',
                      self.log.lines[-1])

    def test_missing_statement_json_error(self):
        result, _ = self.fetch({'problem.html': JSON_ERROR})

        self.assertIsNone(result)
        self.assertIn('no statement, get-file problem.html replied error ERR_INV_FILE_NAME Invalid file name',
                      self.log.lines[-1])

    def test_not_html(self):
        result, _ = self.fetch({'problem.html': ('application/octet-stream', b'<div class="problem-statement"/>')})

        self.assertIsNone(result)

    def test_images(self):
        result, fetched = self.fetch({
            'problem.html': self.page(
                '<img src="a.png"/><img src="a.png"/><img src="dir/b.png?x=1" style="width: 5em"/>'
                '<img src="gone.png"/><img src="http://example.com/c.png"/>'
                '<img src="/moodle_probpics/1/d.png"/><img src="data:image/png;base64,AA=="/>'),
            'a.png': ('image/png', b'A'),
            'b.png': ('image/png', b'B'),
        })

        # every attachment is fetched once
        self.assertEqual(fetched, ['problem.html', 'a.png', 'b.png', 'gone.png'])
        self.assertEqual(result.images, {'a.png': b'A', 'b.png': b'B'})
        self.assertEqual(result.missing_images, ['gone.png'])
        self.assertTrue(any('warning: problem 3: image gone.png is not in attachments' in line
                            for line in self.log.lines))
        content = result.content_for(7)
        self.assertEqual(content.count('src="/moodle_probpics/7/a.png"'), 2)
        self.assertIn('src="/moodle_probpics/7/b.png"', content)
        self.assertIn('src="/moodle_probpics/7/gone.png"', content)
        # external and inline images are left alone
        self.assertIn('src="http://example.com/c.png"', content)
        self.assertIn('src="/moodle_probpics/1/d.png"', content)
        self.assertIn('src="data:image/png;base64,AA=="', content)
        # every image fits the page, keeping its own style
        self.assertEqual(content.count('style="max-width: 100%; height: auto"'), 6)
        self.assertIn('style="max-width: 100%; height: auto; width: 5em"', content)

    def test_processing(self):
        result, _ = self.fetch({'problem.html': self.page(
            '<div class="header"><div class="title">A</div></div>'
            '<p>$$$x$$$ and $$$$$$y$$$$$$</p>'
            '<div class="sample-tests"><pre>1</pre></div>')})

        content = result.content_for(7)
        self.assertNotIn('class="header"', content)
        self.assertNotIn('sample-tests', content)
        # as the filesystem reload converts it
        self.assertIn('\\(x\\) and \\(\\)y\\(\\)', content)
        self.assertEqual(result.sample_tests_html,
                         '<div class="problem-statement"><div class="sample-tests"><pre>1</pre></div></div>')

    def test_no_samples(self):
        result, _ = self.fetch({'problem.html': self.page('<p>text</p>')})

        self.assertEqual(result.sample_tests_html, '')
