import datetime

from dateutil.tz import UTC
from mock import MagicMock

from rmatics import db
from rmatics import monitor_cacher
from rmatics.model import Run, MonitorCourseModule, CourseModule
from rmatics.model.monitor import MonitorStatement, Monitor
from rmatics.testutils import TestCase
from rmatics.view import get_problems_by_statement_id
from rmatics.view.monitors.monitor import get_runs, ContestBasedMonitorAPIView

MONITOR_GROUP_ID = 5


class TestLoadProblemsByCourseModule(TestCase):
    def setUp(self):
        super().setUp()
        self.create_statements()
        self.create_ejudge_problems()
        self.create_problems()
        self.create_statement_problems()
        self.create_course_module_statement()
        self.create_course_module_monitor()
        self.create_monitor_statements()

    def create_course_module_monitor(self):
        self.monitor = Monitor()
        db.session.add(self.monitor)
        db.session.flush()
        self.monitor_cm = MonitorCourseModule(group_id=MONITOR_GROUP_ID, monitor_id=self.monitor.id)
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

    def test_get_problems_by_statement_cm(self):
        group_id, statement_ids = ContestBasedMonitorAPIView._get_contests(
            self.course_module_statement.id
        )
        self.assertIsNone(group_id)
        self.assertEqual(statement_ids, [self.statements[0].id])

    def test_get_problems_by_statement(self):
        problems = get_problems_by_statement_id(self.statements[0].id)

        expected_ids = sorted(map(lambda p: p.id, self.problems))

        ids = sorted(map(lambda p: p.id, problems))

        self.assertEqual(ids, expected_ids)

    def test_get_monitor_statements(self):
        group_id, statement_ids = ContestBasedMonitorAPIView._get_contests(
            self.course_module_monitor.id
        )
        expected_ids = {s.id for s in self.statements}
        self.assertEqual(group_id, MONITOR_GROUP_ID)
        self.assertEqual(expected_ids, set(statement_ids))


class TestGenerateMonitor(TestCase):
    def setUp(self):
        super().setUp()

        self.mock_cacher_get = MagicMock(return_value='')
        self.mock_cacher_set = MagicMock()

        monitor_cacher._instance.store.get = self.mock_cacher_get
        monitor_cacher._instance.store.set = self.mock_cacher_set

        self.create_users()
        self.create_statements()
        self.create_ejudge_problems()
        self.create_problems()
        self.create_statement_problems()
        self.create_course_module_statement()
        self.create_runs()

        db.session.commit()

        self.contest_id = self.course_module_statement.id

    def create_runs(self, creation_time=None):
        if not hasattr(self, 'runs'):
            self.runs = []
        runs = [
            Run(problem_id=self.problems[0].id,
                user_id=self.users[0].id),
            Run(problem_id=self.problems[0].id,
                user_id=self.users[1].id),
            Run(problem_id=self.problems[0].id,
                user_id=self.users[2].id),
        ]

        if creation_time is not None:
            for run in runs:
                run.create_time = creation_time

        self.runs += runs

        db.session.add_all(runs)
        db.session.commit()

    def test_get_runs(self):
        user_ids = [user.id for user in self.users]
        serialized_runs = get_runs(problem_id=self.problems[0].id,
                                   user_ids=user_ids)

        self.mock_cacher_get.assert_called_once()
        self.mock_cacher_set.assert_called_once()

        self.assertEqual(len(serialized_runs), 3)

        run = self.runs[0]
        serialized_run = serialized_runs[0]
        expected_run_time = run.create_time.replace(tzinfo=UTC).strftime('%Y-%m-%dT%H:%M:%S%z')
        self.assertIn('user', serialized_run)
        self.assertEqual(serialized_run.get('id'), run.id)
        self.assertEqual(serialized_run.get('problem_id'), run.problem_id)
        self.assertEqual(serialized_run.get('create_time'), expected_run_time)
        self.assertEqual(serialized_run.get('ejudge_score'), run.ejudge_score)
        self.assertEqual(serialized_run.get('ejudge_test_num'), run.ejudge_test_num)

        user = run.user
        serialized_user = serialized_run['user']
        self.assertEqual(serialized_user.get('id'), user.id)
        self.assertEqual(serialized_user.get('firstname'), user.firstname)
        self.assertEqual(serialized_user.get('lastname'), user.lastname)

    def test_get_runs_before(self):
        time_creation = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        time_before = time_creation - datetime.timedelta(hours=1)
        self.create_runs(time_creation)

        user_ids = [user.id for user in self.users]
        runs = get_runs(problem_id=self.problems[0].id,
                        user_ids=user_ids,
                        time_before=int(time_before.timestamp()))

        self.assertEqual(len(runs), 3)

    def test_get_runs_after(self):
        time_creation = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        self.create_runs(time_creation)
        time_after = time_creation + datetime.timedelta(hours=1)

        user_ids = [user.id for user in self.users]
        runs = get_runs(problem_id=self.problems[0].id,
                        user_ids=user_ids,
                        time_after=int(time_after.timestamp()))

        self.assertEqual(len(runs), 3)

    def test_get_runs_before_and_after(self):
        time_creation = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        self.create_runs(time_creation)
        time_after = time_creation + datetime.timedelta(hours=1)

        time_creation = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        self.create_runs(time_creation)
        time_before = time_creation - datetime.timedelta(hours=1)

        user_ids = [user.id for user in self.users]
        runs = get_runs(problem_id=self.problems[0].id,
                        user_ids=user_ids,
                        time_after=int(time_after.timestamp()),
                        time_before=int(time_before.timestamp()))

        self.assertEqual(len(runs), 3)
