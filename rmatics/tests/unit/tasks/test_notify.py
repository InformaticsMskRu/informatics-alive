import mock
from werkzeug.exceptions import BadRequest

from rmatics.model.base import db
from rmatics.model.run import Run
from rmatics.tasks.notify import (
    NON_TERMINAL_STATUSES,
    _get_run,
    check_run,
    invalidate_cache,
    load_protocol,
    make_nonterminal_upd_chain,
    make_terminal_upd_chain,
    upd_run,
    NoRunError,
)
from rmatics.testutils import TestCase
from rmatics.utils.run import EjudgeStatuses

FETCH_PROTOCOL_PATH = 'rmatics.tasks.notify.fetch_protocol'


def notify_data(run, status, **kwargs):
    """Сообщение нотификации в том виде, в котором его шлют
    notify-worker / ejudge-listener."""
    data = {
        'run_id': run.ejudge_run_id,
        'contest_id': run.ejudge_contest_id,
        'run_uuid': run.ejudge_run_uuid,
        'status': status,
        'judge_id': run.judge_id,
        'rmatics_run_id': run.id,
    }
    data.update(kwargs)
    return data


class NotifyTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.create_users()
        self.create_ejudge_problems()
        self.create_judges()

        self.run = Run(
            user_id=self.users[0].id,
            problem_id=self.ejudge_problems[0].id,
            ejudge_contest_id=self.ejudge_problems[0].ejudge_contest_id,
            lang_id=1,
            ejudge_status=EjudgeStatuses.IN_QUEUE.value,
            ejudge_run_id=10,
            ejudge_run_uuid='uuid-10',
            judge_id=1,
        )
        db.session.add(self.run)
        db.session.commit()


class TestGetRun(NotifyTestCase):
    def test_lookup_by_rmatics_run_id(self):
        data = notify_data(self.run, status=0)
        self.assertEqual(_get_run(data).id, self.run.id)

    def test_lookup_by_uuid_and_judge_id(self):
        data = notify_data(self.run, status=0)
        data['rmatics_run_id'] = None
        self.assertEqual(_get_run(data).id, self.run.id)

    def test_missing_run_raises(self):
        data = notify_data(self.run, status=0)
        data['rmatics_run_id'] = 777555
        with self.assertRaises(NoRunError):
            _get_run(data)

    def test_missing_run_by_uuid_raises(self):
        data = notify_data(self.run, status=0)
        data['rmatics_run_id'] = None
        data['run_uuid'] = 'no-such-uuid'
        with self.assertRaises(NoRunError):
            _get_run(data)

    def test_string_typed_ids_still_match(self):
        """Новый ejudge шлёт judge_id / rmatics_run_id строками:
        валидация в _get_run должна приводить их к int, а не падать."""
        data = notify_data(self.run, status=0)
        data['rmatics_run_id'] = str(self.run.id)
        data['judge_id'] = str(self.run.judge_id)
        self.assertEqual(_get_run(data).id, self.run.id)

    def test_string_typed_judge_id_lookup_by_uuid(self):
        """То же, но по ветке (run_uuid, judge_id) — без rmatics_run_id."""
        data = notify_data(self.run, status=0)
        data['rmatics_run_id'] = None
        data['judge_id'] = str(self.run.judge_id)
        self.assertEqual(_get_run(data).id, self.run.id)


class TestUpdRun(NotifyTestCase):
    def test_updates_nonterminal_run(self):
        data = notify_data(self.run, status=EjudgeStatuses.OK.value,
                           score=100, test_num=5)

        upd_run.delay(data)

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status, EjudgeStatuses.OK.value)
        self.assertEqual(run.ejudge_score, 100)
        self.assertEqual(run.ejudge_test_num, 5)

    def test_terminal_status_is_not_overwritten(self):
        """Гонка: запоздавшая нетерминальная нотификация не должна
        перезаписать терминальный статус."""
        upd_run.delay(notify_data(self.run, status=EjudgeStatuses.OK.value))

        upd_run.delay(notify_data(self.run,
                                  status=EjudgeStatuses.RUNNING.value))

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status, EjudgeStatuses.OK.value)

    def test_terminal_overwrites_terminal(self):
        """Rejudge: терминальный статус может смениться другим терминальным
        только после сброса в IN_QUEUE (это делает RunAPI.post)."""
        upd_run.delay(notify_data(self.run, status=EjudgeStatuses.WA.value))

        run = db.session.query(Run).get(self.run.id)
        run.ejudge_status = EjudgeStatuses.IN_QUEUE.value
        db.session.commit()

        upd_run.delay(notify_data(self.run, status=EjudgeStatuses.OK.value))

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status, EjudgeStatuses.OK.value)

    def test_updates_by_uuid_when_no_rmatics_run_id(self):
        """Совместимость со старым ejudge: нет ext_user —
        ищем по (run_uuid, judge_id)."""
        data = notify_data(self.run, status=EjudgeStatuses.OK.value)
        data.pop('rmatics_run_id')

        upd_run.delay(data)

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_status, EjudgeStatuses.OK.value)

    def test_sets_ejudge_run_id_from_notification(self):
        """ejudge_run_id/run_uuid проставляются из нотификации
        (могли не успеть записаться при отправке)."""
        data = notify_data(self.run, status=EjudgeStatuses.RUNNING.value)
        data['run_id'] = 555

        upd_run.delay(data)

        run = db.session.query(Run).get(self.run.id)
        self.assertEqual(run.ejudge_run_id, 555)


class TestCheckRun(NotifyTestCase):
    def test_passes_data_through(self):
        data = notify_data(self.run, status=0)
        result = check_run.apply(args=(data,))
        self.assertEqual(result.get(), data)

    def test_retries_when_run_not_found(self):
        from celery.exceptions import Retry
        data = notify_data(self.run, status=0)
        data['rmatics_run_id'] = 777555

        with mock.patch.object(check_run, 'retry',
                               side_effect=Retry('retry')) as retry_mock:
            with self.assertRaises(Retry):
                check_run.apply(args=(data,))
        retry_mock.assert_called_once()


class TestLoadProtocol(NotifyTestCase):
    @mock.patch(FETCH_PROTOCOL_PATH)
    def test_terminal_status_loads_protocol(self, fetch_mock):
        protocol = {'run_id': self.run.id, 'tests': {}, 'compiler_output': ''}
        fetch_mock.return_value = protocol

        data = notify_data(self.run, status=EjudgeStatuses.OK.value)
        load_protocol.delay(data)

        fetch_mock.assert_called_once_with(
            self.judges[1].url,
            'token-1',
            self.run.ejudge_contest_id,
            self.run.ejudge_run_id,
            self.run.id,
        )

        saved = db.session.query(Run).get(self.run.id).protocol
        self.assertIsNotNone(saved)
        self.assertEqual(saved['run_id'], self.run.id)

    @mock.patch(FETCH_PROTOCOL_PATH)
    def test_nonterminal_status_skips_protocol(self, fetch_mock):
        data = notify_data(self.run, status=EjudgeStatuses.RUNNING.value)
        load_protocol.delay(data)
        fetch_mock.assert_not_called()

    @mock.patch(FETCH_PROTOCOL_PATH)
    def test_fetch_error_goes_to_retry(self, fetch_mock):
        from celery.exceptions import Retry
        fetch_mock.side_effect = ConnectionError('ejudge api down')

        data = notify_data(self.run, status=EjudgeStatuses.OK.value)
        with mock.patch.object(load_protocol, 'retry',
                               side_effect=Retry('retry')) as retry_mock:
            with self.assertRaises(Retry):
                load_protocol.apply(args=(data,))
        retry_mock.assert_called_once()


class TestInvalidateCache(NotifyTestCase):
    def test_invalidates_monitor_cache(self):
        data = notify_data(self.run, status=EjudgeStatuses.OK.value)
        with mock.patch(
                'rmatics.tasks.notify.invalidate_monitor_cache_by_run'
        ) as invalidate_mock:
            invalidate_cache.delay(data)
        invalidate_mock.assert_called_once()


class TestChains(TestCase):
    def test_terminal_chain_contains_load_protocol(self):
        tasks = [t['task'] for t in make_terminal_upd_chain().tasks]
        self.assertEqual(tasks, [
            'rmatics.tasks.notify.check_run',
            'rmatics.tasks.notify.load_protocol',
            'rmatics.tasks.notify.upd_run',
            'rmatics.tasks.notify.invalidate_cache',
        ])

    def test_nonterminal_chain_skips_load_protocol(self):
        tasks = [t['task'] for t in make_nonterminal_upd_chain().tasks]
        self.assertEqual(tasks, [
            'rmatics.tasks.notify.check_run',
            'rmatics.tasks.notify.upd_run',
            'rmatics.tasks.notify.invalidate_cache',
        ])

    def test_non_terminal_statuses(self):
        self.assertEqual(NON_TERMINAL_STATUSES, {96, 98, 377})
