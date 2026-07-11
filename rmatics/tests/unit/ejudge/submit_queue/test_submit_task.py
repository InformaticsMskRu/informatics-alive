import datetime

import mock

from rmatics.ejudge.submit_queue.task import submit_task
from rmatics.model.base import db
from rmatics.model.run import Run
from rmatics.testutils import TestCase
from rmatics.utils.run import EjudgeStatuses

SUBMIT_PATH = 'rmatics.ejudge.submit_queue.task.submit'

EJUDGE_ERROR_RESPONSE = {
    'code': 105,
    'message': 'Отправка бинарного файла',
}


class SubmitTaskTestCase(TestCase):
    """Общий setUp: пользователи, задачи, judges-конфиг и один Run."""

    def setUp(self):
        super().setUp()
        self.create_users()
        self.create_ejudge_problems()
        self.create_statements()
        self.create_judges()

        self.run = self._make_run()

    def _make_run(self, lang_id=27):
        run = Run(
            user_id=self.users[0].id,
            problem_id=self.ejudge_problems[0].id,
            statement_id=self.statements[0].id,
            create_time=datetime.datetime(2026, 7, 10, 12, 0, 0),
            ejudge_contest_id=self.ejudge_problems[0].ejudge_contest_id,
            lang_id=lang_id,
            ejudge_status=EjudgeStatuses.IN_QUEUE.value,
        )
        db.session.add(run)
        db.session.flush()
        run.update_source(b'source')
        db.session.commit()
        return run


class TestSubmitTaskSuccess(SubmitTaskTestCase):

    @mock.patch(SUBMIT_PATH)
    def test_sends_to_default_judge(self, submit_mock):
        submit_mock.return_value = {
            'code': 0,
            'run_id': 77,
            'run_uuid': 'uuid-77',
        }

        submit_task.delay(self.run.id)

        submit_mock.assert_called_once_with(
            run_file=b'source',
            contest_id=self.ejudge_problems[0].ejudge_contest_id,
            prob_id=self.ejudge_problems[0].problem_id,
            lang_id=27,
            filename='common_filename',
            url=self.judges[1].url,
            sender_user_id=self.judges[1].sender_user_id,
            token='token-1',
            ext_user_id=self.run.id,
        )

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_run_id, 77)
        self.assertEqual(run.ejudge_run_uuid, 'uuid-77')
        self.assertEqual(run.judge_id, 1)
        # статус не трогаем: его проставит нотификация от ejudge
        self.assertEqual(run.ejudge_status, EjudgeStatuses.IN_QUEUE.value)

    @mock.patch(SUBMIT_PATH)
    def test_judges_settings_reroute(self, submit_mock):
        """judges_settings задачи перенаправляет посылку в другой ejudge
        с маппингом contest_id/problem_id/lang_id."""
        submit_mock.return_value = {'code': 0, 'run_id': 1, 'run_uuid': 'u'}

        problem = self.ejudge_problems[0]
        problem.judges_settings = [
            {'judge_id': 2, 'contest_id': 500, 'problem_id': 6},
        ]
        db.session.commit()

        submit_task.delay(self.run.id)

        submit_mock.assert_called_once_with(
            run_file=b'source',
            contest_id=500,
            prob_id=6,
            lang_id=62,  # judges[2].lang_map: 27 -> 62
            filename='common_filename',
            url=self.judges[2].url,
            sender_user_id=7,
            token='token-2',
            ext_user_id=self.run.id,
        )

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.judge_id, 2)


class TestSubmitTaskErrors(SubmitTaskTestCase):

    @mock.patch(SUBMIT_PATH)
    def test_ejudge_error_code_sets_error_status_and_protocol(self, submit_mock):
        submit_mock.return_value = EJUDGE_ERROR_RESPONSE

        submit_task.delay(self.run.id)

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status,
                         EjudgeStatuses.RMATICS_SUBMIT_ERROR.value)

        protocol = run.protocol
        self.assertIsNotNone(protocol)
        self.assertEqual(protocol['compiler_output'],
                         EJUDGE_ERROR_RESPONSE['message'])
        self.assertEqual(protocol['run_id'], self.run.id)

    @mock.patch(SUBMIT_PATH)
    def test_none_response_sets_error_status(self, submit_mock):
        """ejudge_proxy.submit возвращает None (например, нет токена) —
        задача не должна падать."""
        submit_mock.return_value = None

        submit_task.delay(self.run.id)

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status,
                         EjudgeStatuses.RMATICS_SUBMIT_ERROR.value)
        protocol = run.protocol
        self.assertEqual(protocol['compiler_output'], 'Ошибка отправки посылки')

    @mock.patch(SUBMIT_PATH)
    def test_submit_exception_goes_to_retry(self, submit_mock):
        """Исключение при отправке уходит в celery retry,
        статус посылки не меняется."""
        from celery.exceptions import Retry

        submit_mock.side_effect = ConnectionError('ejudge is down')

        with self.assertRaises(Retry):
            submit_task.apply(args=(self.run.id,))

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status, EjudgeStatuses.IN_QUEUE.value)

    @mock.patch(SUBMIT_PATH)
    def test_submit_exception_after_max_retries_sets_error(self, submit_mock):
        """После исчерпания ретраев — RMATICS_SUBMIT_ERROR и протокол
        с ошибкой отправки."""
        submit_mock.side_effect = ConnectionError('ejudge is down')

        submit_task.apply(args=(self.run.id,),
                          retries=submit_task.max_retries)

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status,
                         EjudgeStatuses.RMATICS_SUBMIT_ERROR.value)
        self.assertEqual(run.protocol['compiler_output'],
                         'Ошибка отправки посылки')

    @mock.patch(SUBMIT_PATH)
    def test_run_not_found_does_not_submit(self, submit_mock):
        submit_task.delay(999999)
        submit_mock.assert_not_called()

    @mock.patch(SUBMIT_PATH)
    def test_no_judge_and_no_default_does_not_submit(self, submit_mock):
        """У задачи нет judges_settings, а DEFAULT_JUDGE_ID не настроен —
        посылка не отправляется, задача не падает."""
        self.app.config['DEFAULT_JUDGE_ID'] = None

        submit_task.delay(self.run.id)

        submit_mock.assert_not_called()
        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status, EjudgeStatuses.IN_QUEUE.value)

    @mock.patch(SUBMIT_PATH)
    def test_unknown_judge_does_not_submit(self, submit_mock):
        """judge_id не из judges.json — посылка не отправляется."""
        problem = self.ejudge_problems[0]
        problem.judges_settings = [
            {'judge_id': 99, 'contest_id': 1, 'problem_id': 1},
        ]
        db.session.commit()

        submit_task.delay(self.run.id)
        submit_mock.assert_not_called()
