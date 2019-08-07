import datetime
from unittest.mock import call

import mock
from flask import url_for
from mock import MagicMock

from rmatics import db, monitor_cacher
from rmatics.model import Run, UserGroup, MonitorCourseModule, CourseModule
from rmatics.model.monitor import MonitorStatement, Monitor
from rmatics.testutils import TestCase


class TestContestBasedMonitorGetApi(TestCase):
    def setUp(self):
        super().setUp()

        self.mock_cacher_get = MagicMock(return_value='')
        self.mock_cacher_set = MagicMock()

        monitor_cacher._instance.store.get = self.mock_cacher_get
        monitor_cacher._instance.store.set = self.mock_cacher_set

        self.create_users()
        self.create_groups()

        for user in self.users:
            ug = UserGroup(user_id=user.id, group_id=self.groups[0].id)
            db.session.add(ug)

        self.create_statements()
        self.create_ejudge_problems()
        self.create_problems()
        self.create_statement_problems()
        self.create_course_module_statement()
        self.create_runs()

        db.session.commit()

        self.course_module_statement_id = self.course_module_statement.id

        self.create_course_module_monitor()
        self.create_monitor_statements()

    def create_course_module_monitor(self):
        self.monitor = Monitor()
        db.session.add(self.monitor)
        db.session.flush()
        self.monitor_cm = MonitorCourseModule(group_id=self.groups[0].id, monitor_id=self.monitor.id)
        db.session.add(self.monitor_cm)
        db.session.flush()
        self.course_module_monitor = CourseModule(instance_id=self.monitor.id,
                                                  module=MonitorCourseModule.MODULE)
        db.session.add(self.course_module_monitor)
        db.session.commit()

    def create_monitor_statements(self):
        self.monitor_statements = [
            MonitorStatement(
                statement_id=statement.id,
                monitor_id=self.monitor_cm.id)
            for statement in self.statements
        ]
        db.session.add_all(self.monitor_statements)
        db.session.commit()

    def create_runs(self):
        self.runs = [
            Run(problem_id=self.problems[0].id,
                user_id=self.users[0].id),
            Run(problem_id=self.problems[0].id,
                user_id=self.users[1].id),
            Run(problem_id=self.problems[0].id,
                user_id=self.users[2].id),
        ]
        db.session.add_all(self.runs)

    def send_request(self, **data):
        url = url_for('monitor.crud', **data)
        resp = self.client.get(url)
        return resp

    def test_simple(self):
        # Run-ы созданы в utcnow()
        time_after = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        time_before = datetime.datetime.utcnow() + datetime.timedelta(days=1)

        resp = self.send_request(contest_id=self.course_module_statement_id, group_id=self.groups[0].id,
                                 time_after=int(time_after.timestamp()),
                                 time_before=int(time_before.timestamp()), )
        self.assert200(resp)

        self.assertEqual(self.mock_cacher_get.call_count, 3)
        self.assertEqual(self.mock_cacher_set.call_count, 3)

        self.assertIn('data', resp.json)
        data = resp.json['data']

        self.assertEqual(len(data), 3)

        for contest_data in data:
            self.assertIn('problem', contest_data)
            self.assertIn('runs', contest_data)
            self.assertIn('contest_id', contest_data)

        problem_data = data[0]['problem']
        self.assertIn('rank', problem_data)

        runs = map(lambda d: d['runs'], data)
        runs_lens = map(len, runs)

        self.assertEqual(sum(runs_lens), 3)

    def test_without_group(self):
        resp = self.send_request(contest_id=self.course_module_statement_id)
        self.assert200(resp)
        data = resp.json['data']

        self.assertEqual(len(data), 3)

    def test_with_course_module_monitor(self):
        resp = self.send_request(contest_id=self.course_module_monitor.id)
        self.assert200(resp)
        data = resp.json['data']
        runs = map(lambda d: d['runs'], data)
        runs_lens = map(len, runs)
        self.assertEqual(sum(runs_lens), 3)


class TestProblemBasedMonitorGetApi(TestCase):
    def setUp(self):
        super().setUp()

    def send_request(self, **data):
        url = url_for('monitor.problem_monitor', **data)
        resp = self.client.get(url)
        return resp

    def test_simple(self):
        runs_data = {'abc': 'def'}
        problem_ids = [1, 2, 3]
        user_ids = [4, 5, 6]
        time_before = 12345
        time_after = 7890
        data = {
            'user_id': user_ids,
            'problem_id': problem_ids,
            'time_before': time_before,
            'time_after': time_after,
        }
        with mock.patch('rmatics.view.monitors.monitor.get_runs') as mock_get_runs:
            mock_get_runs.return_value = runs_data
            resp = self.send_request(**data)

        # As we request runs for three problems,
        # get_runs should be called three times
        calls = [
            call(problem_id=p,
                 user_ids=user_ids,
                 time_before=time_before,
                 time_after=time_after,
                 context_id=None,
                 context_source=None,
                 show_hidden=None)
            for p in problem_ids
        ]
        mock_get_runs.assert_has_calls(calls)

        self.assert200(resp)
        response = resp.json.get('data')

        expected_runs_data = [
            {
                'problem_id': i,
                'runs': [runs_data]
            }
            for i in problem_ids]

        self.assertEqual(response, expected_runs_data)
