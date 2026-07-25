from flask import url_for

from rmatics import db
from rmatics.model import Run
from rmatics.testutils import TestCase
from rmatics.utils.run import EjudgeStatuses


class TestTrustedTokenAuth(TestCase):
    """Ручки trusted api (в них ходит только pynformatics) закрыты
    общим TRUSTED_TOKEN."""

    def setUp(self):
        super().setUp()

        self.create_ejudge_problems()

    def get_problem(self, headers=None):
        url = url_for('problem.problem', problem_id=self.ejudge_problems[0].id)
        return self.client.get(url, headers=headers or {})

    def test_without_token_is_unauthorized(self):
        self.assert401(self.get_problem())

    def test_wrong_scheme_is_unauthorized(self):
        self.assert401(self.get_problem({'Authorization': 'Token trusted-token'}))

    def test_wrong_token_is_forbidden(self):
        self.assert403(self.get_problem({'Authorization': 'Bearer not-a-token'}))

    def test_valid_token_passes(self):
        self.assert200(self.get_problem(self.trusted_headers))

    def test_judge_token_is_not_enough(self):
        """Токен ejudge api не пускает в trusted api."""
        self.create_judges()
        self.assert403(self.get_problem(self.judge_headers(1)))

    def test_not_configured_token_is_forbidden(self):
        self.app.config['TRUSTED_TOKEN'] = None
        self.assert403(self.get_problem(self.trusted_headers))

    def test_monitor_is_closed_too(self):
        url = url_for('monitor.problem_monitor',
                      problem_id=self.ejudge_problems[0].id)
        self.assert401(self.client.get(url))
        self.assert200(self.client.get(url, headers=self.trusted_headers))


class TestJudgeTokenAuth(TestCase):
    """Ручки нотификаций закрыты токеном ejudge api, своим у каждого judge."""

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

    def send_notification(self, headers=None, **kwargs):
        data = {
            'run_id': 10,
            'contest_id': self.run.ejudge_contest_id,
            'run_uuid': 'uuid-10',
            'status': EjudgeStatuses.RUNNING.value,
            'judge_id': 1,
            'rmatics_run_id': self.run.id,
        }
        data.update(kwargs)
        url = url_for('problem.update_from_ejudge_v2')
        return self.client.post(url, json=data, headers=headers or {})

    def test_without_token_is_unauthorized(self):
        self.assert401(self.send_notification())

    def test_own_judge_token_passes(self):
        self.assert200(self.send_notification(headers=self.judge_headers(1)))

    def test_another_judge_token_is_forbidden(self):
        """Токен второго ejudge не подходит к нотификации от первого."""
        self.assert403(self.send_notification(headers=self.judge_headers(2)))

    def test_trusted_token_is_not_enough(self):
        self.assert403(self.send_notification(headers=self.trusted_headers))

    def test_unknown_judge_is_forbidden(self):
        self.assert403(self.send_notification(headers=self.judge_headers(1),
                                              judge_id=777))

    def test_without_judge_id_is_forbidden(self):
        """judge_id обязателен: без него токен не с чем сверять."""
        self.assert403(self.send_notification(headers=self.judge_headers(1),
                                              judge_id=None))

    def test_v1_requires_judge_id_too(self):
        url = url_for('problem.update_from_ejudge')
        data = {
            'run_id': 10,
            'contest_id': self.run.ejudge_contest_id,
            'status': EjudgeStatuses.OK.value,
        }
        self.assert403(self.client.post(url, json=data,
                                        headers=self.judge_headers(1)))
        self.assert200(self.client.post(url, json={**data, 'judge_id': 1},
                                        headers=self.judge_headers(1)))
