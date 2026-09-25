from flask import url_for

from rmatics.ejudge.judges_config import JudgeLang
from rmatics.testutils import TestCase


class TestJudges(TestCase):
    def send_request(self, **kwargs):
        url = url_for('judges.judges')
        return self.client.get(url, **kwargs)

    def test_returns_public_metadata(self):
        self.create_judges()

        resp = self.send_request()

        self.assert200(resp)
        self.assertEqual('success', resp.json['status'])
        data = resp.json['data']

        self.assertEqual(set(data.keys()), {'1', '2'})
        self.assertEqual(data['2']['name'], 'second-ejudge')
        self.assertEqual(data['2']['url'], 'http://ejudge-2/cgi-bin/new-client')

    def test_secret_fields_are_not_exposed(self):
        self.create_judges()

        resp = self.send_request()

        for judge in resp.json['data'].values():
            self.assertNotIn('token', judge)
            self.assertNotIn('sender_user_id', judge)

    def test_empty_when_no_judges_configured(self):
        resp = self.send_request()

        self.assert200(resp)
        self.assertEqual(resp.json['data'], {})

    def test_langs(self):
        self.create_judges()
        self.judges[2].langs = {27: JudgeLang('Python 3.9', 62)}

        data = self.send_request().json['data']

        self.assertIsNone(data['1']['langs'])
        self.assertEqual(data['2']['langs'],
                         {'27': {'name': 'Python 3.9', 'ejudge_lang_id': 62}})
