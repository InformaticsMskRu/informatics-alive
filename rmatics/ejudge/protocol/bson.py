import bson

from rmatics.utils.run import get_string_status

def _to_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _text(elem) -> str:
    if elem is None:
        return ''
    data = elem.get('data')
    if data is None:
        return ''
    return data.decode("utf-8")


def _too_big(elem) -> bool:
    return elem is not None and elem.get('too_big') == True

def parse_bson_testing_report(bts: bytes, run_id: int) -> dict:
    data = bson.decode_all(bts)[0]

    tests = {}

    tests_elem = data.get('tests')
    if tests_elem is not None:
        for test in tests_elem:
            num = test.get('num')
            if num is None:
                continue

            inp = test.get('input')
            out = test.get('output')
            corr = test.get('correct')
            checker = test.get('checker')
            err = test.get('stderr')

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
                'real_time': _to_int(test.get('real_time'), 0),
                'max_memory_used': _to_int(
                    test.get('max_memory_used'),
                    0,
                ),
            }

    b_output = data.get('compiler_output')

    return {
        'run_id': run_id,
        'compiler_output': b_output.decode("utf-8") if b_output is not None else None,
        'tests': tests,
    }