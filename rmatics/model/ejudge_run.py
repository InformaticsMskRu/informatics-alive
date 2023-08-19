import xml.dom.minidom
import xml
import zipfile
from flask import g

from rmatics.model.base import db
from rmatics.utils.ejudge_archive import EjudgeArchiveReader
from rmatics.utils.functions import attrs_to_dict
from rmatics.utils.run import *


class EjudgeRun(db.Model):
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['contest_id', 'prob_id'],
            ['moodle.mdl_ejudge_problem.ejudge_contest_id', 'moodle.mdl_ejudge_problem.problem_id']
        ),
        db.ForeignKeyConstraint(
            ['user_id'],
            ['moodle.mdl_user.ej_id']
        ),
        {'schema': 'ejudge'}
    )
    __tablename__ = 'runs'

    run_id = db.Column(db.Integer, primary_key=True)
    contest_id = db.Column(db.Integer, primary_key=True)
    size = db.Column(db.Integer)
    create_time = db.Column(db.DateTime)
    create_nsec = db.Column(db.Integer)
    user_id = db.Column(db.Integer)
    prob_id = db.Column(db.Integer) # TODO: rename to problem_id
    lang_id = db.Column(db.Integer)
    status = db.Column(db.Integer)
    ssl_flag = db.Column(db.Integer)
    ip_version = db.Column(db.Integer)
    ip = db.Column(db.String(64))
    hash = db.Column(db.String(128))
    run_uuid = db.Column(db.String(40))
    score = db.Column(db.Integer)
    test_num = db.Column(db.Integer)
    score_adj = db.Column(db.Integer)
    locale_id = db.Column(db.Integer)
    judge_id = db.Column(db.Integer)
    variant = db.Column(db.Integer)
    pages = db.Column(db.Integer)
    is_imported = db.Column(db.Integer)
    is_hidden = db.Column(db.Integer)
    is_readonly = db.Column(db.Integer)
    is_examinable = db.Column(db.Integer)
    mime_type = db.Column(db.String(64))
    examiners0 = db.Column(db.Integer)
    examiners1 = db.Column(db.Integer)
    examiners2 = db.Column(db.Integer)
    exam_score0 = db.Column(db.Integer)
    exam_score1 = db.Column(db.Integer)
    exam_score2 = db.Column(db.Integer)
    last_change_time = db.Column(db.DateTime)
    last_change_nsec = db.Column(db.Integer)
    is_marked = db.Column(db.Integer)
    is_saved = db.Column(db.Integer)
    saved_status = db.Column(db.Integer)
    saved_score = db.Column(db.Integer)
    saved_test = db.Column(db.Integer)
    passed_mode = db.Column(db.Integer)
    eoln_type = db.Column(db.Integer)
    store_flags = db.Column(db.Integer)
    token_flags = db.Column(db.Integer)
    token_count = db.Column(db.Integer)

    comments = db.relationship('Comment', backref=db.backref('comments'))
    user = db.relationship('SimpleUser', backref=db.backref('simpleuser'), uselist=False)
    problem = db.relationship('EjudgeProblem', backref=db.backref('ejudge_runs', lazy='dynamic'), uselist=False)

    SIGNAL_DESCRIPTION = {
        1: "Hangup detected on controlling terminal or death of controlling process",
        2: "Interrupt from keyboard",
        3: "Quit from keyboard",
        4: "Illegal Instruction",
        6: "Abort signal",
        7: "Bus error (bad memory access)",
        8: "Floating point exception",
        9: "Kill signal",
        11: "Invalid memory reference",
        13: "Broken pipe: write to pipe with no readers",
        14: "Timer signal",
        15: "Termination signal"
    }

    @db.reconstructor
    def init_on_load(self):
        self.out_path = "/home/judges/{0:06d}/var/archive/output/{1}/{2}/{3}/{4:06d}.zip".format(
            self.contest_id,
            to32(self.run_id // (32 ** 3) % 32),
            to32(self.run_id // (32 ** 2) % 32),
            to32(self.run_id // 32 % 32),
            self.run_id
        )
        self._out_arch = None
        self._out_arch_file_names = set()

    @lazy
    def get_audit(self):
        data = safe_open(submit_path(audit_path, self.contest_id, self.run_id), 'r').read()
        if type(data) == bytes:
            data = data.decode('ascii')
        return data
    @lazy
    def get_sources(self):
        data = safe_open(submit_path(sources_path, self.contest_id, self.run_id), 'rb').read()
        for encoding in ['utf-8', 'ascii', 'windows-1251']:
            try:
                data = data.decode(encoding)
            except:
                print('decoded:', encoding)
                pass
            else:
                break
        else:
            return 'Ошибка кодировки'
        return data

    def get_output_file(self, test_num, tp="o", size=None): #tp: o - output, e - stderr, c - checker
        data = self.get_output_archive().getfile("{0:06}.{1}".format(test_num, tp)).decode('ascii')
        if size is not None:
            data = data[:size]
        return data

    def get_output_file_size(self, test_num, tp="o"): #tp: o - output, e - stderr, c - checker
        data = self.get_output_archive().getfile("{0:06}.{1}".format(test_num, tp)).decode('ascii')
        return len(data)

    def get_output_archive(self):
        if "output_archive" not in self.__dict__:
            self.output_archive = EjudgeArchiveReader(submit_path(output_path, self.contest_id, self.run_id))
        return self.output_archive

    def get_test_full_protocol(self, test_num):
        """
        Возвращает полный протокол по номеру теста
        :param test_num: - str
        """
        judge_info = self.judge_tests_info[test_num]
        test_protocol = self.problem.get_test_full(test_num)

        test_protocol.update(self.tests[test_num])

        test_protocol['big_output'] = False
        try:
            if self.get_output_file_size(int(test_num), tp='o') <= 255:
                test_protocol['output'] = self.get_output_file(int(test_num), tp='o')
            else:
                test_protocol['output'] = self.get_output_file(int(test_num), tp='o', size=255) + '...\n'
                test_protocol['big_output'] = True
        except OSError as e:
            test_protocol['output'] = judge_info.get('output', '')

        try:
            if self.get_output_file_size(int(test_num), tp='c') <= 255:
                test_protocol['checker_output'] = self.get_output_file(int(test_num), tp='c')
            else:
                test_protocol['checker_output'] = self.get_output_file(int(test_num), tp='c', size=255) + '...\n'
        except OSError as e:
            test_protocol['checker_output'] = judge_info.get('checker', '')

        try:
            if self.get_output_file_size(int(test_num), tp='e') <= 255:
                test_protocol['error_output'] = self.get_output_file(int(test_num), tp='e')
            else:
                test_protocol['error_output'] = self.get_output_file(int(test_num), tp='e', size=255) + '...\n'
        except OSError as e:
            test_protocol['error_output'] = judge_info.get('stderr', '')

        if 'term-signal' in judge_info:
            test_protocol['extra'] = 'Signal %(signal)s. %(description)s' % {
                'signal': judge_info['term-signal'],
                'description': self.SIGNAL_DESCRIPTION[judge_info['term-signal']],
            }
        if 'exit-code' in judge_info:
            test_protocol['extra'] = test_protocol.get('extra', '') + '\n Exit code %(exit_code)s. ' % {
                'exit_code': judge_info['exit-code']
            }

        for type_ in [('o', 'output'), ('c', 'checker_output'), ('e', 'error_output')]:
            file_name = '{0:06d}.{1}'.format(int(test_num), type_[0])
            if self._out_arch is None:
                try:
                    self._out_arch = zipfile.ZipFile(self.out_path, 'r')
                    self._out_arch_file_names = set(self._out_arch.namelist())
                except:
                    pass
            if file_name not in self._out_arch_file_names or type_[1] in test_protocol:
                continue
            with self._out_arch.open(file_name, 'r') as f:
                test_protocol[type_[1]] = f.read(1024).decode("utf-8") + "...\n"

        return test_protocol

    def parsetests(self):
        """
        Parse tests data from xml archive
        """
        self.test_count = 0
        self.tests = {}
        self.judge_tests_info = {}
        self.status_string = None
        self.compiler_output = None
        self.host = None
        self.maxtime = None

        if self.xml:
            rep = self.xml.getElementsByTagName('testing-report')[0]
            self.tests_count = int(rep.getAttribute('run-tests'))
            self.status_string = rep.getAttribute('status')

            compiler_output_elements = self.xml.getElementsByTagName('compiler_output')
            if compiler_output_elements and self.status_string == "CE":
                self.compiler_output = getattr(compiler_output_elements[0].firstChild, 'nodeValue', '')

            host_elements = self.xml.getElementsByTagName('host')
            if host_elements:
                self.host = host_elements[0].firstChild.nodeValue

            for node in self.xml.getElementsByTagName('test'):
                number = node.getAttribute('num')
                status = node.getAttribute('status')
                time = node.getAttribute('time')
                real_time = node.getAttribute('real-time')
                max_memory_used = node.getAttribute('max-memory-used')
                self.test_count += 1

                try:
                   time = int(time)
                except ValueError:
                   time = 0

                try:
                   real_time = int(real_time)
                except ValueError:
                   real_time = 0

                test = {
                    'status': status,
                    'string_status': get_string_status(status),
                    'real_time': real_time,
                    'time': time,
                    'max_memory_used' : max_memory_used,
                }
                judge_info = {}

                for _type in ('input', 'output', 'correct', 'stderr', 'checker'):
                    lst = node.getElementsByTagName(_type)
                    if lst and lst[0].firstChild:
                        judge_info[_type] = lst[0].firstChild.nodeValue
                    else:
                        judge_info[_type] = ''

                if node.hasAttribute('term-signal'):
                    judge_info['term-signal'] = int(node.getAttribute('term-signal'))
                if node.hasAttribute('exit-code'):
                    judge_info['exit-code'] = int(node.getAttribute('exit-code'))

                self.judge_tests_info[number] = judge_info
                self.tests[number] = test
            try:
                #print([test['time'] for test in self.tests.values()] + [test['real_time'] for test in self.tests.values()])
                self.maxtime = max([test['time'] for test in self.tests.values()] + [test['real_time'] for test in self.tests.values()])
            except ValueError:
                pass

    @staticmethod
    def get_by(run_id, contest_id):
        try:
            return db.session.query(EjudgeRun).filter(EjudgeRun.run_id == int(run_id)).filter(EjudgeRun.contest_id == int(contest_id)).first()
        except:
            return None

    @lazy
    def _get_compilation_protocol(self):
        filename = submit_path(protocols_path, self.contest_id, self.run_id)
        if filename:
            if os.path.isfile(filename):
                myopen = lambda x,y : open(x, y, encoding='utf-8')
            else:
                filename += '.gz'
                myopen = gzip.open
            try:
                xml_file = myopen(filename, 'r')
                try:
                    res = xml_file.read()
                    try:
                        res = res.decode('cp1251').encode('utf8')
                    except:
                        pass

                    try:
                        return str(res, encoding='UTF-8')
                    except TypeError:
                        try:
                            res = res.decode('cp1251').encode('utf8')
                        except Exception:
                            pass
                        return res
                except Exception as e:
                    return e
            except IOError as e:
                return e
        else:
            return ''

    @lazy
    def _get_protocol(self):
        filename = submit_path(protocols_path, self.contest_id, self.run_id)
        if filename != '':
            return get_protocol_from_file(filename)
        else:
            return '<a></a>'

    protocol = property(_get_protocol)
    compilation_protocol = property(_get_compilation_protocol)

    @lazy
    def fetch_tested_protocol_data(self):
        self.xml = xml.dom.minidom.parseString(str(self.protocol))
        self.parsetests()

    def _set_output_archive(self, val):
        self.output_archive = val

    def get_pynformatics_run(self):
        if self.pynformatics_run:
            return self.pynformatics_run

        pynformatics_run = PynformaticsRun(
            run=self,
            source=self.get_sources(),
        )
        db.session.add(pynformatics_run)
        return pynformatics_run

    def serialize(self, attributes=None):
        if not attributes:
            attributes = (
                'run_id',
                'contest_id',
                'create_time',
                'lang_id',
                'prob_id',
                'score',
                'size',
                'status',
                'problem_id',
            )

        serialized = attrs_to_dict(self, *attributes)

        if 'create_time' in attributes:
            serialized['create_time'] = str(serialized['create_time'])

        if 'problem_id' in attributes:
            serialized['problem_id'] = self.problem.id

        serialized.update(self.get_pynformatics_run().serialize())

        user = getattr(g, 'user', None)
        if not user or user.ejudge_id != self.user.ejudge_id:
            serialized['user'] = self.user.serialize()

        return serialized
