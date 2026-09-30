import base64
import itertools
from unittest import mock

import requests
from flask import url_for
from sqlalchemy import text

from rmatics.ejudge import ejudge_api, problem_import, statement
from rmatics.model.base import db
from rmatics.model.ejudge_contest import EjudgeContest
from rmatics.model.problem import EjudgeProblem, Problem
from rmatics.testutils import TestCase

DEFAULT_JUDGE = 1
OTHER_JUDGE = 2
CONTEST = 2395
PROB = 3

# a get-problem-json reply: ejudge doesn't serialize short_name (a char[] field)
EJUDGE_PROBLEM = {
    'id': PROB,
    'long_name': 'Sum',
    'internal_name': 'sum',
    'extid': 'polygon:123',
    'uuid': 'u-1',
    'type': 'standard',
    'time_limit_millis': 1500,
    'max_vm_size': 268435456,
}


def reply(payload, status_code=200):
    resp = mock.Mock(status_code=status_code)
    resp.json.return_value = payload
    return resp


def file_reply(content_type, body):
    return mock.Mock(status_code=200, headers={'Content-Type': content_type}, content=body)


# what get-file answers for a file missing from attachments/
ERROR_PAGE = file_reply('text/html; charset=utf-8', b'<html><body>Operation failed</body></html>')

STATEMENT_HTML = """<html><body><div class="problem-statement">
<div class="header"><div class="title">C. Sum</div></div>
<div class="legend"><p>Find $$$a+b$$$.</p><img class="tex-graphics" src="pic.png"/></div>
<div class="sample-tests"><div class="section-title">Examples</div><pre>1 2</pre></div>
</div></body></html>""".encode()


class TestReloadProblem(TestCase):
    def setUp(self):
        super().setUp()
        self.create_judges()  # judge 1 is the default one
        self.ejudge_problem = dict(EJUDGE_PROBLEM)
        self.contest_problems = [
            dict(EJUDGE_PROBLEM, id=1, long_name='First'),
            dict(EJUDGE_PROBLEM, id=2, long_name='Second'),
        ]
        self.contest_name = 'Contest 2395'
        # attachments/ of every ejudge problem: name -> reply
        self.attachments = {}

        # the test schema has no AUTO_INCREMENT on the composite primary
        # keys, so the rows get explicit ids
        ids = itertools.count(100)
        insert_row = problem_import._insert_row

        def insert_with_id(table, values):
            row_id = next(ids)
            insert_row(table, {**values, 'id': row_id})
            return row_id

        patcher = mock.patch.object(problem_import, '_insert_row', side_effect=insert_with_id)
        patcher.start()
        self.addCleanup(patcher.stop)

    def ejudge_get(self, url, params, headers, timeout):
        if params['action'] == 'get-problem-json':
            return reply({'ok': True, 'problem': self.ejudge_problem})
        if params['action'] == 'list-problems-json':
            return reply({'ok': True, 'problems': self.contest_problems})
        if params['action'] == 'contest-status-json':
            return reply({'ok': True, 'result': {'contest': {'name': self.contest_name}}})
        if params['action'] == 'get-file':
            return self.attachments.get(params['file'], ERROR_PAGE)
        raise AssertionError(f'unexpected action {params["action"]}')

    def send_request(self, judge_id=OTHER_JUDGE, contest_id=CONTEST, problem_id=PROB,
                     ejudge_get=None):
        if problem_id is None:
            url = url_for('contest.ejudge_reload_contest', judge_id=judge_id,
                          contest_id=contest_id)
        else:
            url = url_for('contest.ejudge_reload_problem', judge_id=judge_id,
                          contest_id=contest_id, problem_id=problem_id)
        with mock.patch.object(ejudge_api.requests, 'get',
                               side_effect=ejudge_get or self.ejudge_get) as get:
            resp = self.client.post(url)
        self.ejudge_calls = [call[1]['params']['action'] for call in get.call_args_list
                             if call[1]['params']['action'] != 'get-file']
        self.fetched_files = [call[1]['params']['file'] for call in get.call_args_list
                              if call[1]['params']['action'] == 'get-file']
        db.session.expire_all()
        return resp

    def create_problem(self, ejudge_prid, ejudge_contest_id=0, problem_id=0,
                       judges_settings=None, name='Old', short_id='X'):
        return EjudgeProblem.create(
            ejudge_prid=ejudge_prid, contest_id=0, ejudge_contest_id=ejudge_contest_id,
            problem_id=problem_id, judges_settings=judges_settings, name=name,
            ejudge_name=name, short_id=short_id, timelimit=1, memorylimit=1024,
            content='<p>kept</p>',
        )

    def set_raw_settings(self, problem, raw):
        db.session.execute(
            text('UPDATE moodle.mdl_ejudge_problem SET judges_settings = :raw WHERE id = :id'),
            {'raw': raw, 'id': problem.ejudge_prid})
        db.session.commit()

    def get_problem(self, problem_id) -> EjudgeProblem:
        return db.session.query(EjudgeProblem).filter(EjudgeProblem.id == problem_id).one()

    def test_unknown_judge(self):
        resp = self.send_request(judge_id=9)

        self.assert404(resp)
        self.assertEqual(self.ejudge_calls, [])

    def test_creates_problem_of_other_judge(self):
        resp = self.send_request()

        self.assert200(resp)
        data = resp.json['data']
        self.assertEqual(data['action'], 'create')
        self.assertEqual(data['ejudge_problem']['extid'], 'polygon:123')

        problem = self.get_problem(data['problems'][0]['id'])
        self.assertEqual(problem.judges_settings,
                         [{'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB}])
        self.assertEqual((problem.name, problem.ejudge_name), ('Sum', 'Sum'))
        self.assertIsNone(problem.short_id)
        self.assertEqual(problem.timelimit, 1.5)
        self.assertEqual(problem.memorylimit, 268435456)
        self.assertFalse(problem.output_only)
        self.assertTrue(problem.hidden)
        self.assertEqual(problem.content, problem_import.NO_STATEMENT)
        # no legacy address: those columns name problems of the default judge
        self.assertEqual((problem.contest_id, problem.ejudge_contest_id, problem.problem_id),
                         (0, 0, 0))
        self.assertEqual(db.session.query(EjudgeContest).count(), 0)
        self.assertEqual(self.ejudge_calls, ['get-problem-json'])

    def test_creates_problem_of_default_judge_with_contest(self):
        resp = self.send_request(judge_id=DEFAULT_JUDGE)

        self.assert200(resp)
        problem = self.get_problem(resp.json['data']['problems'][0]['id'])
        contest = db.session.query(EjudgeContest).one()
        self.assertEqual((contest.ejudge_int_id, contest.ejudge_id, contest.name),
                         (CONTEST, '002395', 'Contest 2395'))
        self.assertEqual((problem.contest_id, problem.ejudge_contest_id, problem.problem_id),
                         (contest.id, CONTEST, PROB))
        self.assertEqual(problem.judges_settings,
                         [{'judge_id': DEFAULT_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB}])

    def test_default_judge_reuses_existing_contest(self):
        contest = EjudgeContest(name='Existing', ejudge_int_id=CONTEST)
        contest.id = 7
        db.session.add(contest)
        db.session.commit()

        resp = self.send_request(judge_id=DEFAULT_JUDGE)

        problem = self.get_problem(resp.json['data']['problems'][0]['id'])
        self.assertEqual(problem.contest_id, 7)
        self.assertEqual(db.session.query(EjudgeContest).count(), 1)
        self.assertEqual(self.ejudge_calls, ['get-problem-json'])

    def test_updates_mapped_problem(self):
        settings = [
            {'judge_id': DEFAULT_JUDGE, 'contest_id': 10, 'problem_id': 1},
            {'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB,
             'lang_ids': [27]},
        ]
        existing = self.create_problem(1, judges_settings=settings)

        resp = self.send_request()

        self.assert200(resp)
        data = resp.json['data']
        self.assertEqual(data['action'], 'update')
        self.assertEqual(data['problems'], [{'id': existing.id, 'name': 'Sum'}])
        problem = self.get_problem(existing.id)
        self.assertEqual((problem.name, problem.timelimit), ('Sum', 1.5))
        # routing and the statement are left alone
        self.assertEqual(problem.judges_settings, settings)
        self.assertEqual(problem.content, '<p>kept</p>')
        self.assertEqual(db.session.query(Problem).count(), 1)

    def test_detects_entry_written_by_hand(self):
        existing = self.create_problem(1)
        self.set_raw_settings(
            existing, '[{"problem_id": "3", "contest_id": "2395", "judge_id": 2}]')

        resp = self.send_request()

        self.assertEqual(resp.json['data']['action'], 'update')
        self.assertEqual(resp.json['data']['problems'][0]['id'], existing.id)

    def test_similar_entries_are_not_matches(self):
        self.create_problem(1, judges_settings=[
            {'judge_id': OTHER_JUDGE, 'contest_id': 23950, 'problem_id': PROB}])
        self.create_problem(2, judges_settings=[
            {'judge_id': DEFAULT_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB}])
        self.create_problem(3, judges_settings=[
            {'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': 4}])
        broken = self.create_problem(4)
        self.set_raw_settings(broken, '{"judge_id": 2, "contest_id": 2395}')

        resp = self.send_request()

        self.assertEqual(resp.json['data']['action'], 'create')
        self.assertEqual(db.session.query(Problem).count(), 5)

    def test_legacy_problem_of_default_judge_gets_entry(self):
        existing = self.create_problem(1, ejudge_contest_id=CONTEST, problem_id=PROB)

        resp = self.send_request(judge_id=DEFAULT_JUDGE)

        self.assertEqual(resp.json['data']['action'], 'update')
        problem = self.get_problem(existing.id)
        self.assertEqual(problem.name, 'Sum')
        self.assertEqual(problem.judges_settings,
                         [{'judge_id': DEFAULT_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB}])

    def test_legacy_columns_do_not_match_other_judge(self):
        self.create_problem(1, ejudge_contest_id=CONTEST, problem_id=PROB)

        resp = self.send_request(judge_id=OTHER_JUDGE)

        self.assertEqual(resp.json['data']['action'], 'create')

    def test_legacy_problem_routed_elsewhere_is_not_a_match(self):
        self.create_problem(1, ejudge_contest_id=CONTEST, problem_id=PROB, judges_settings=[
            {'judge_id': OTHER_JUDGE, 'contest_id': 500, 'problem_id': 6}])

        resp = self.send_request(judge_id=DEFAULT_JUDGE)

        self.assertEqual(resp.json['data']['action'], 'create')

    def test_entries_per_language_list_the_problem_once(self):
        existing = self.create_problem(1, judges_settings=[
            {'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB, 'lang_ids': [3]},
            {'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB, 'lang_ids': [27]},
        ])

        resp = self.send_request()

        self.assertEqual(resp.json['data']['problems'], [{'id': existing.id, 'name': 'Sum'}])

    def test_updates_every_mapped_problem(self):
        entry = {'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB}
        first = self.create_problem(1, judges_settings=[entry])
        second = self.create_problem(2, judges_settings=[entry])

        resp = self.send_request()

        self.assertEqual([p['id'] for p in resp.json['data']['problems']],
                         [first.id, second.id])

    def test_update_keeps_short_id_ejudge_does_not_send(self):
        existing = self.create_problem(1, ejudge_contest_id=CONTEST, problem_id=PROB, short_id='C')

        resp = self.send_request(judge_id=DEFAULT_JUDGE)

        self.assertEqual(resp.json['data']['action'], 'update')
        self.assertEqual(self.get_problem(existing.id).short_id, 'C')

    def test_short_name_is_used_when_sent(self):
        self.ejudge_problem = dict(EJUDGE_PROBLEM, short_name='D')
        existing = self.create_problem(1, ejudge_contest_id=CONTEST, problem_id=PROB, short_id='C')

        self.send_request(judge_id=DEFAULT_JUDGE)

        self.assertEqual(self.get_problem(existing.id).short_id, 'D')

    def test_problem_fields(self):
        self.ejudge_problem = {'type': 'output-only', 'time_limit': 2}

        resp = self.send_request()

        problem = self.get_problem(resp.json['data']['problems'][0]['id'])
        # a problem must not have an empty name
        self.assertEqual((problem.name, problem.ejudge_name), (problem_import.NO_NAME, ''))
        self.assertEqual(problem.timelimit, 2)
        self.assertIsNone(problem.memorylimit)
        self.assertTrue(problem.output_only)

    def test_ejudge_problem_not_found(self):
        resp = self.send_request(ejudge_get=lambda *a, **kw: reply(
            {'ok': False, 'error': {'symbol': 'ERR_INV_PROB_ID'}}, 404))

        self.assert404(resp)
        self.assertEqual(db.session.query(Problem).count(), 0)

    def test_ejudge_unavailable(self):
        def down(*args, **kwargs):
            raise requests.ConnectionError('down')

        resp = self.send_request(ejudge_get=down)

        self.assertStatus(resp, 502)
        self.assertEqual(db.session.query(Problem).count(), 0)

    def test_contest_imports_every_problem(self):
        existing = self.create_problem(1, judges_settings=[
            {'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': 2}])

        resp = self.send_request(problem_id=None)

        self.assert200(resp)
        results = resp.json['data']['problems']
        self.assertEqual([(r['ejudge_problem']['id'], r['action']) for r in results],
                         [(1, 'create'), (2, 'update')])
        self.assertEqual(results[1]['problems'], [{'id': existing.id, 'name': 'Second'}])
        created = self.get_problem(results[0]['problems'][0]['id'])
        self.assertEqual(created.name, 'First')
        self.assertEqual(created.judges_settings,
                         [{'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': 1}])
        # the listed problems are used as is, without a request per problem
        self.assertEqual(self.ejudge_calls, ['list-problems-json'])

    def test_contest_of_default_judge_creates_contest_once(self):
        resp = self.send_request(judge_id=DEFAULT_JUDGE, problem_id=None)

        self.assert200(resp)
        contest = db.session.query(EjudgeContest).one()
        for result in resp.json['data']['problems']:
            problem = self.get_problem(result['problems'][0]['id'])
            self.assertEqual((problem.contest_id, problem.ejudge_contest_id),
                             (contest.id, CONTEST))
        self.assertEqual(self.ejudge_calls, ['list-problems-json', 'contest-status-json'])

    def test_empty_contest(self):
        self.contest_problems = []

        resp = self.send_request(problem_id=None)

        self.assert200(resp)
        self.assertEqual(resp.json['data'], {'problems': []})

    def test_contest_not_found(self):
        resp = self.send_request(problem_id=None, ejudge_get=lambda *a, **kw: reply(
            {'ok': False, 'error': {'symbol': 'ERR_INV_CONTEST_ID'}}, 404))

        self.assert404(resp)

    def test_contest_problem_without_id(self):
        self.contest_problems = [{'long_name': 'No id'}]

        resp = self.send_request(problem_id=None)

        self.assertStatus(resp, 502)
        self.assertEqual(db.session.query(Problem).count(), 0)

    def hold_import_lock(self, judge_id=OTHER_JUDGE, contest_id=CONTEST):
        """Hold the contest lock from another connection, as a concurrent reload does."""
        name = f'rmatics:ejudge_import:{judge_id}:{contest_id}'
        conn = db.engine.connect()
        self.assertEqual(conn.execute(text('SELECT GET_LOCK(:name, 0)'), name=name).scalar(), 1)

        def release():
            conn.execute(text('SELECT RELEASE_LOCK(:name)'), name=name)
            conn.close()
        return release

    def test_locked_contest_is_conflict(self):
        release = self.hold_import_lock()
        try:
            with mock.patch.object(problem_import, 'IMPORT_LOCK_TIMEOUT', 0):
                problem_resp = self.send_request()
                contest_resp = self.send_request(problem_id=None)
        finally:
            release()

        self.assertStatus(problem_resp, 409)
        self.assertStatus(contest_resp, 409)
        self.assertEqual(db.session.query(Problem).count(), 0)

        self.assert200(self.send_request())

    def test_lock_is_per_contest_and_judge(self):
        release = self.hold_import_lock(contest_id=CONTEST + 1)
        self.addCleanup(release)

        with mock.patch.object(problem_import, 'IMPORT_LOCK_TIMEOUT', 0):
            self.assert200(self.send_request())
            self.assert200(self.send_request(judge_id=DEFAULT_JUDGE))

    def test_lock_is_released_after_failure(self):
        def contest_status_fails(url, params, headers, timeout):
            if params['action'] == 'contest-status-json':
                return reply({'ok': False, 'error': {'symbol': 'ERR_INTERNAL'}}, 500)
            return self.ejudge_get(url, params, headers, timeout)

        # the contest row of the default judge is created under the lock
        failed = self.send_request(judge_id=DEFAULT_JUDGE, ejudge_get=contest_status_fails)
        self.assertStatus(failed, 502)

        with mock.patch.object(problem_import, 'IMPORT_LOCK_TIMEOUT', 0):
            self.assert200(self.send_request(judge_id=DEFAULT_JUDGE))

    def add_statement(self):
        self.attachments = {
            statement.STATEMENT_FILE: file_reply('text/html', STATEMENT_HTML),
            'pic.png': file_reply('image/png', b'PNG'),
        }

    def test_statement_of_created_problem(self):
        self.add_statement()

        resp = self.send_request()

        self.assert200(resp)
        data = resp.json['data']
        self.assertEqual((data['statement'], data['missing_images']), ('imported', []))
        result, = data['problems']
        self.assertEqual(result['images'], {'pic.png': base64.b64encode(b'PNG').decode()})
        problem = self.get_problem(result['id'])
        self.assertIn(f'src="/moodle_probpics/{problem.id}/pic.png"', problem.content)
        self.assertIn('Find \\(a+b\\).', problem.content)
        self.assertNotIn('C. Sum', problem.content)
        self.assertNotIn('1 2', problem.content)
        self.assertIn('1 2', problem.sample_tests_html)

    def test_statement_replaces_content_of_updated_problem(self):
        existing = self.create_problem(1, judges_settings=[
            {'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB}])
        self.add_statement()

        resp = self.send_request()

        self.assertEqual(resp.json['data']['action'], 'update')
        self.assertIn(f'/moodle_probpics/{existing.id}/pic.png', self.get_problem(existing.id).content)

    def test_no_statement_keeps_content(self):
        existing = self.create_problem(1, judges_settings=[
            {'judge_id': OTHER_JUDGE, 'contest_id': CONTEST, 'problem_id': PROB}])

        resp = self.send_request()

        data = resp.json['data']
        self.assertEqual(data['statement'], 'not found')
        self.assertNotIn('images', data['problems'][0])
        self.assertEqual(self.get_problem(existing.id).content, '<p>kept</p>')

    def test_contest_fetches_every_statement(self):
        self.add_statement()

        resp = self.send_request(problem_id=None)

        self.assert200(resp)
        self.assertEqual([r['statement'] for r in resp.json['data']['problems']],
                         ['imported', 'imported'])
        self.assertEqual(self.fetched_files, ['problem.html', 'pic.png'] * 2)

    def test_get_is_not_allowed(self):
        url = url_for('contest.ejudge_reload_problem', judge_id=OTHER_JUDGE,
                      contest_id=CONTEST, problem_id=PROB)

        self.assert405(self.client.get(url))
