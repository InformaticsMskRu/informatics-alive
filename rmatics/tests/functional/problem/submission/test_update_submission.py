import mock
from flask import url_for
from mock import patch, MagicMock

from rmatics import db, mongo
from rmatics.model import Run
from rmatics.model.rejudge import Rejudge
from rmatics.testutils import TestCase


class TestAPIUpdateRun(TestCase):
    def setUp(self):
        super().setUp()

        self.create_ejudge_problems()
        self.create_problems()
        self.create_users()

        self.run = Run(user_id=self.users[0].id, problem_id=self.problems[1].id,
                       ejudge_status=1, lang_id=1)
        db.session.add(self.run)
        db.session.commit()

    def send_request(self, run_id, data: dict):
        url = url_for('problem.run', run_id=run_id)
        resp = self.client.put(url, json=data)
        return resp

    def test_put_not_found_run(self):
        run_id = 777555
        resp = self.send_request(run_id, {})
        self.assert404(resp)

    def test_update_run(self):
        run_id = self.run.id
        with patch('rmatics.utils.cacher.helpers.monitor_cacher') as mc:
            mc.invalidate_all_of = MagicMock()
            self.monitor_invalidate_cache_mock = mc.invalidate_all_of
            resp = self.send_request(run_id, {'ejudge_status': '1488'})
        self.assert200(resp)

        run = db.session.query(Run).get(run_id)
        self.assertEqual(run.ejudge_status, 1488)
        self.monitor_invalidate_cache_mock.assert_called_once()


class TestRejudgeAPI(TestCase):
    def setUp(self):
        super().setUp()

        self.create_ejudge_problems()
        self.create_problems()
        self.create_users()

    def send_request(self, run_id):
        url = url_for('problem.rejudge_run', run_id=run_id)
        resp = self.client.post(url)
        return resp

    @mock.patch('rmatics.view.problem.run.queue_submit')
    def test_simple(self, queue_submit_mock):
        run = Run(user_id=self.users[0].id, problem_id=self.problems[1].id,
                  ejudge_status=1, lang_id=1, ejudge_contest_id=1,
                  ejudge_url='ej_url')
        db.session.add(run)
        db.session.commit()

        protocol = {'my_protocol': 'data', 'run_id': run.id}
        mongo.db.protocol.insert_one(protocol)
        del protocol['_id']

        resp = self.send_request(run.id)

        self.assert200(resp)

        queue_submit_mock.assert_called_once()

        rejudge = db.session.query(Rejudge) \
            .filter(Rejudge.run_id == run.id) \
            .filter(Rejudge.ejudge_contest_id == run.ejudge_contest_id) \
            .one()

        old_protocol = mongo.db.rejudge.find_one({'rejudge_id': rejudge.id})

        del old_protocol['_id']
        del old_protocol['rejudge_id']
        self.assertEqual(old_protocol, protocol)

    @mock.patch('rmatics.view.problem.run.get_judge')
    @mock.patch('rmatics.view.problem.run.queue_submit')
    def test_rejudge_named_judge_run(self, queue_submit_mock, get_judge_mock):
        from rmatics.ejudge.judges_config import JudgeConfig
        judge_url = 'http://named-judge/cgi-bin/new-client'
        get_judge_mock.return_value = JudgeConfig(url=judge_url)

        run = Run(user_id=self.users[0].id, problem_id=self.problems[1].id,
                  ejudge_status=1, lang_id=1, ejudge_contest_id=1,
                  judge_id='named-judge')
        db.session.add(run)
        db.session.commit()

        protocol = {'my_protocol': 'data', 'run_id': run.id}
        mongo.db.protocol.insert_one(protocol)
        del protocol['_id']

        resp = self.send_request(run.id)

        self.assert200(resp)
        get_judge_mock.assert_called_once_with('named-judge')
        queue_submit_mock.assert_called_once_with(run.id, judge_url)

        rejudge = db.session.query(Rejudge) \
            .filter(Rejudge.run_id == run.id) \
            .filter(Rejudge.ejudge_contest_id == run.ejudge_contest_id) \
            .one()
        self.assertEqual(rejudge.ejudge_url, judge_url)

        old_protocol = mongo.db.rejudge.find_one({'rejudge_id': rejudge.id})
        del old_protocol['_id']
        del old_protocol['rejudge_id']
        self.assertEqual(old_protocol, protocol)

    @mock.patch('rmatics.view.problem.run.queue_submit')
    def test_rejudge_failed_run(self, queue_submit_mock):
        run = Run(user_id=self.users[0].id, problem_id=self.problems[1].id,
                  ejudge_status=1, lang_id=1, ejudge_contest_id=1,
                  ejudge_url=None)
        db.session.add(run)
        db.session.commit()

        protocol = {'my_protocol': 'data', 'run_id': run.id}
        mongo.db.protocol.insert_one(protocol)
        del protocol['_id']

        resp = self.send_request(run.id)

        self.assert200(resp)

        queue_submit_mock.assert_called_once()

        rejudge = db.session.query(Rejudge) \
            .filter(Rejudge.run_id == run.id) \
            .filter(Rejudge.ejudge_contest_id == run.ejudge_contest_id) \
            .one_or_none()

        self.assertIsNone(rejudge)
