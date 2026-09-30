import unittest
from unittest import mock

import requests

from rmatics.ejudge import ejudge_api
from rmatics.ejudge.ejudge_api import EjudgeApiError, EjudgeNotFound
from rmatics.ejudge.judges_config import JudgeConfig

JUDGE = JudgeConfig(url='http://ejudge-2/cgi-bin/new-master', token='token-2')


def reply(payload, status_code=200):
    resp = mock.Mock(status_code=status_code)
    resp.json.return_value = payload
    return resp


class TestEjudgeApi(unittest.TestCase):
    def call(self, func, *args, resp=None, side_effect=None):
        with mock.patch.object(ejudge_api.requests, 'get',
                               return_value=resp, side_effect=side_effect) as get:
            return func(JUDGE, *args), get

    def test_get_problem_request(self):
        problem, get = self.call(ejudge_api.get_problem, 2395, 3,
                                 resp=reply({'ok': True, 'problem': {'id': 3}}))

        self.assertEqual(problem, {'id': 3})
        url, = get.call_args[0]
        self.assertEqual(url, 'http://ejudge-2/cgi-bin/new-master')
        self.assertEqual(get.call_args[1]['params'], {
            'action': 'get-problem-json', 'contest_id': 2395, 'prob_id': 3,
            'abstract': 0, 'size_mode': 1,
        })
        self.assertEqual(get.call_args[1]['headers'], {'Authorization': 'Bearer token-2'})

    def test_list_problems(self):
        problems, get = self.call(ejudge_api.list_problems, 2395, resp=reply(
            {'ok': True, 'problems': [{'id': 1}, {'id': 2}]}))

        self.assertEqual(problems, [{'id': 1}, {'id': 2}])
        self.assertEqual(get.call_args[1]['params'],
                         {'action': 'list-problems-json', 'contest_id': 2395, 'size_mode': 1})

    def test_list_problems_without_problems(self):
        with self.assertRaises(EjudgeApiError):
            self.call(ejudge_api.list_problems, 2395, resp=reply({'ok': True}))

    def test_get_file(self):
        resp = mock.Mock(status_code=200, content=b'PNG',
                         headers={'Content-Type': 'image/PNG; charset=binary'})
        (content_type, body), get = self.call(ejudge_api.get_file, 2395, 3, 'pic.png', resp=resp)

        self.assertEqual((content_type, body), ('image/png', b'PNG'))
        self.assertEqual(get.call_args[1]['params'], {
            'action': 'get-file', 'contest_id': 2395, 'prob_id': 3, 'file': 'pic.png'})
        self.assertEqual(get.call_args[1]['headers'], {'Authorization': 'Bearer token-2'})

    def test_get_file_connection_error(self):
        with self.assertRaises(EjudgeApiError):
            self.call(ejudge_api.get_file, 2395, 3, 'pic.png',
                      side_effect=requests.ConnectionError('down'))

    def test_get_contest_name(self):
        name, get = self.call(ejudge_api.get_contest_name, 2395, resp=reply({
            'ok': True, 'result': {'contest': {'id': 2395, 'name': 'Contest'}},
        }))

        self.assertEqual(name, 'Contest')
        self.assertEqual(get.call_args[1]['params'],
                         {'action': 'contest-status-json', 'contest_id': 2395})

    def test_404_is_not_found(self):
        with self.assertRaises(EjudgeNotFound):
            self.call(ejudge_api.get_problem, 2395, 99, resp=reply(
                {'ok': False, 'error': {'num': 5, 'symbol': 'ERR_INV_PROB_ID'}}, 404))

    def test_error_reply(self):
        with self.assertRaises(EjudgeApiError) as ctx:
            self.call(ejudge_api.get_problem, 2395, 3, resp=reply(
                {'ok': False, 'error': {'symbol': 'ERR_PERMISSION_DENIED',
                                        'message': 'Permission denied'}}, 403))

        self.assertNotIsInstance(ctx.exception, EjudgeNotFound)
        self.assertIn('ERR_PERMISSION_DENIED', str(ctx.exception))

    def test_non_json_reply(self):
        resp = reply(None)
        resp.json.side_effect = ValueError('no json')
        with self.assertRaises(EjudgeApiError):
            self.call(ejudge_api.get_problem, 2395, 3, resp=resp)

    def test_connection_error(self):
        with self.assertRaises(EjudgeApiError):
            self.call(ejudge_api.get_problem, 2395, 3,
                      side_effect=requests.ConnectionError('down'))

    def test_reply_without_problem(self):
        with self.assertRaises(EjudgeApiError):
            self.call(ejudge_api.get_problem, 2395, 3, resp=reply({'ok': True}))
