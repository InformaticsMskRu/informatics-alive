import base64
import bz2
import unittest

import bson

from rmatics.ejudge.protocol.bson import parse_bson_testing_report
from rmatics.ejudge.protocol.xml import parse_xml_testing_report

RUN_ID = 42

XML_REPORT = """<?xml version="1.0" encoding="utf-8"?>
<testing-report run-id="7" judge-id="1">
  <tests>
    <test num="1" status="OK" time="15" real-time="20" max-memory-used="1024">
      <input>1 2</input>
      <output>3</output>
      <correct>3</correct>
      <checker>ok</checker>
    </test>
    <test num="2" status="WA" time="10" real-time="12">
      <input too-big="1"></input>
      <output>4</output>
      <correct>5</correct>
      <checker>wrong answer</checker>
      <stderr>debug output</stderr>
    </test>
  </tests>
  <compiler_output>warning: unused variable</compiler_output>
</testing-report>
"""


class TestXmlParser(unittest.TestCase):
    def setUp(self):
        self.parsed = parse_xml_testing_report(XML_REPORT, RUN_ID)

    def test_run_id(self):
        self.assertEqual(self.parsed['run_id'], RUN_ID)

    def test_compiler_output(self):
        self.assertEqual(self.parsed['compiler_output'],
                         'warning: unused variable')

    def test_tests_parsed(self):
        tests = self.parsed['tests']
        self.assertEqual(set(tests.keys()), {'1', '2'})

        first = tests['1']
        self.assertEqual(first['status'], 'OK')
        self.assertEqual(first['input'], '1 2')
        self.assertEqual(first['output'], '3')
        self.assertEqual(first['corr'], '3')
        self.assertEqual(first['checker_output'], 'ok')
        self.assertEqual(first['time'], 15)
        self.assertEqual(first['real_time'], 20)
        self.assertEqual(first['max_memory_used'], 1024)
        self.assertFalse(first['big_input'])

    def test_too_big_flag(self):
        second = self.parsed['tests']['2']
        self.assertTrue(second['big_input'])
        self.assertEqual(second['error_output'], 'debug output')

    def test_string_status(self):
        self.assertEqual(self.parsed['tests']['1']['string_status'], 'OK')
        self.assertEqual(self.parsed['tests']['2']['string_status'],
                         'Неправильный ответ')

    def test_empty_report(self):
        parsed = parse_xml_testing_report(
            '<testing-report></testing-report>', RUN_ID)
        self.assertEqual(parsed['tests'], {})


def _bson_report(**kwargs):
    doc = {
        'run_id': 7,
        'compiler_output': b'compiled ok',
        'tests': [
            {
                'num': 1,
                'status': 'OK',
                'time': 15,
                'real_time': 20,
                'max_memory_used': 1024,
                'input': {'data': '1 2'},
                'output': {'data': '3'},
                'correct': {'data': '3'},
                'checker': {'data': 'ok'},
            },
        ],
    }
    doc.update(kwargs)
    return bson.BSON.encode(doc)


class TestBsonParser(unittest.TestCase):
    def test_basic(self):
        parsed = parse_bson_testing_report(_bson_report(), RUN_ID)

        self.assertEqual(parsed['run_id'], RUN_ID)
        self.assertEqual(parsed['compiler_output'], 'compiled ok')

        test = parsed['tests']['1']
        self.assertEqual(test['status'], 'OK')
        self.assertEqual(test['string_status'], 'OK')
        self.assertEqual(test['input'], '1 2')
        self.assertEqual(test['output'], '3')
        self.assertEqual(test['corr'], '3')
        self.assertEqual(test['time'], 15)
        self.assertEqual(test['real_time'], 20)
        self.assertEqual(test['max_memory_used'], 1024)

    def test_compiler_output_str(self):
        """compiler_output может прийти и как str — не должен ломать
        парсер (ловилось на реальном ejudge)."""
        parsed = parse_bson_testing_report(
            _bson_report(compiler_output='plain string'), RUN_ID)
        self.assertEqual(parsed['compiler_output'], 'plain string')

    def test_no_compiler_output(self):
        raw = _bson_report()
        doc = bson.decode_all(raw)[0]
        del doc['compiler_output']
        parsed = parse_bson_testing_report(bson.BSON.encode(doc), RUN_ID)
        self.assertIsNone(parsed['compiler_output'])

    def test_base64_and_bzip2_data(self):
        data = 'big test input'
        encoded = base64.b64encode(bz2.compress(data.encode()))
        parsed = parse_bson_testing_report(_bson_report(tests=[{
            'num': 1,
            'status': 'OK',
            'input': {'data': encoded, 'base64': True, 'bzip2': True},
        }]), RUN_ID)
        self.assertEqual(parsed['tests']['1']['input'], data)

    def test_too_big_flag(self):
        parsed = parse_bson_testing_report(_bson_report(tests=[{
            'num': 1,
            'status': 'OK',
            'input': {'too_big': True},
        }]), RUN_ID)
        self.assertTrue(parsed['tests']['1']['big_input'])
        self.assertEqual(parsed['tests']['1']['input'], '')

    def test_empty_tests(self):
        parsed = parse_bson_testing_report(_bson_report(tests=[]), RUN_ID)
        self.assertEqual(parsed['tests'], {})
