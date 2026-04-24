import datetime

import mock
import sys
from hamcrest import (
    assert_that,
    anything,
    calling,
    equal_to,
    is_not,
    raises,
)

from rmatics.model.base import db
from rmatics.model.run import Run
from rmatics.model.ejudge_run import EjudgeRun, EjudgeStatuses
from rmatics.testutils import TestCase


if 'rmatics.ejudge.submit_queue.submit' in sys.modules:
    del sys.modules['rmatics.ejudge.submit_queue.submit']
with mock.patch('rmatics.ejudge.ejudge_proxy.submit') as ejudge_submit_mock:
    from rmatics.ejudge.submit_queue.submit import Submit


class TestEjudge__submit_queue_submit_send(TestCase):
    def setUp(self):
        super(TestEjudge__submit_queue_submit_send, self).setUp()

        ejudge_submit_mock.reset_mock()

        self.create_users()
        self.create_ejudge_problems()
        self.create_statements()

        self.run = EjudgeRun(
            run_id=12,
            user=self.users[0],
            problem=self.ejudge_problems[0],
        )
        db.session.add(self.run)
        db.session.flush([self.run])

        self.file_mock = mock.Mock()
        # self.file_mock.value.decode.return_value = 'source'
        self.file_mock.filename = 'filename'
        self.file_mock.read.return_value = b'source'

    def test_simple(self):
        run = Run(
            user_id=self.users[0].id,
            problem_id=self.ejudge_problems[0].id,
            statement_id=self.statements[0].id,
            create_time=datetime.datetime(2018, 3, 30, 16, 59, 0),
            ejudge_contest_id=self.ejudge_problems[0].ejudge_contest_id,
            lang_id=27,
            ejudge_status=EjudgeStatuses.COMPILING.value,
        )
        db.session.add(run)
        db.session.flush()
        db.session.refresh(run)

        file = self.file_mock

        text = file.read()
        run.update_source(text)

        submit = Submit(
            id=1,
            run_id=run.id,
            ejudge_url='ejudge_url',
        )

        ejudge_submit_mock.return_value = {
            'code': 0,
            'run_id': self.run.run_id,
        }

        submit.send()

        from flask import current_app

        ejudge_submit_mock.assert_called_once_with(
            run_file=b'source',
            contest_id=1,
            prob_id=1,
            lang_id=27,
            filename='common_filename',
            url='ejudge_url',
            sender_user_id=5,
            token=None,
        )

        run = db.session.query(Run).one()
        assert_that(run.ejudge_run_id, equal_to(self.run.run_id))
        assert_that(run.ejudge_contest_id, equal_to(self.ejudge_problems[0].ejudge_contest_id))
        assert_that(run.user.id, equal_to(self.users[0].id))
        assert_that(run.problem.id, equal_to(self.ejudge_problems[0].id))
        assert_that(run.judge_id, equal_to(None))  # DEFAULT_JUDGE_ID not configured in tests

    def test_handles_submit_exception(self):
        # В случае, если функция submit бросила исключение
        run = Run(
            user_id=self.users[0].id,
            problem_id=self.ejudge_problems[0].id,
            statement_id=self.statements[0].id,
            create_time=datetime.datetime(2018, 3, 30, 16, 59, 0),
            ejudge_contest_id=self.ejudge_problems[0].ejudge_contest_id,
            lang_id=27,
            ejudge_status=EjudgeStatuses.COMPILING.value,
        )
        db.session.add(run)
        db.session.flush()
        db.session.refresh(run)

        file = self.file_mock

        text = file.read()
        run.update_source(text)

        submit = Submit(
            id=1,
            run_id=run.id,
            ejudge_url='ejudge_url',
        )

        ejudge_submit_mock.side_effect = lambda *args, **kwargs: 1 / 0
        assert_that(
            calling(submit.send),
            is_not(raises(anything())),
        )


        ejudge_submit_mock.side_effect = None

    def test_handles_submit_error(self):
        # В случае, если ejudge вернул не 0 код

        run = Run(
            user_id=self.users[0].id,
            problem_id=self.ejudge_problems[0].id,
            statement_id=self.statements[0].id,
            create_time=datetime.datetime(2018, 3, 30, 17, 10, 11),
            ejudge_contest_id=self.ejudge_problems[0].ejudge_contest_id,
            lang_id=27,
            ejudge_status=EjudgeStatuses.COMPILING.value,
        )
        db.session.add(run)
        db.session.flush()
        db.session.refresh(run)

        file = self.file_mock

        text = file.read()
        run.update_source(text)

        submit = Submit(
            id=1,
            run_id=run.id,
            ejudge_url='ejudge_url',
        )

        ejudge_submit_mock.return_value = {
            'code': 123,
            'message': 'some message',
            'other': 'secrets'
        }
        assert_that(
            calling(submit.send),
            is_not(raises(anything())),
        )

        ejudge_submit_mock.side_effect = None
