import json

from mock import MagicMock

from rmatics.testutils import TestCase
from rmatics.utils.cacher.cacher import Cacher, get_cache_key

PREFIX = 'my_cache'
FUNC_NAME = 'my_func_name'
FUNC_RETURN_VALUE = {'data': 'hi!'}


class TestCacher(TestCase):

    def setUp(self):
        super().setUp()

    def test_generate_cache_key(self):
        func = MagicMock(__name__='func')
        prefix = 'prefix'

        args = ['1', 2, False]
        kwargs = {
            'kwarg1': '1',
            'kwarg2': 2,
            'kwarg3': False
        }

        keys = set()

        # Generate unique keys for various args sets
        keys.add(get_cache_key(func, prefix, args, kwargs))

        args[2] = True
        keys.add(get_cache_key(func, prefix, args, kwargs))

        kwargs['kwarg3'] = True
        keys.add(get_cache_key(func, prefix, args, kwargs))

        self.assertEqual(len(keys), 3)
