import xml.etree.ElementTree as ET

from rmatics.utils.run import get_string_status

def _to_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _text(elem) -> str:
    return (elem.text or '') if elem is not None else ''


def _too_big(elem) -> bool:
    return elem is not None and elem.get('too-big') in ('1', 'yes', 'true')

def parse_xml_testing_report(xml_text: str, run_id: int) -> dict:
    root = ET.fromstring(xml_text)

    tests = {}

    tests_elem = root.find('tests')
    if tests_elem is not None:
        for test in tests_elem.findall('test'):
            num = test.get('num')
            if num is None:
                continue

            inp = test.find('input')
            out = test.find('output')
            corr = test.find('correct')
            checker = test.find('checker')
            err = test.find('stderr')

            status_code = test.get('status', 'UNKOWN')

            try:
                string_status = get_string_status(status_code)
            except KeyError:
                string_status = status_code

            tests[num] = {
                'input': _text(inp),
                'big_input': _too_big(inp),
                'corr': _text(corr),
                'big_corr': _too_big(corr),
                'output': _text(out),
                'big_output': _too_big(out),
                'checker_output': _text(checker),
                'error_output': _text(err),
                'extra': '',
                'status': status_code,
                'string_status': string_status,
                'time': _to_int(test.get('time'), 0),
                'real_time': _to_int(test.get('real-time'), 0),
                'max_memory_used': _to_int(
                    test.get('max-memory-used'),
                    0,
                ),
            }

    return {
        'run_id': run_id,
        'compiler_output': _text(root.find('compiler_output')),
        'tests': tests,
    }