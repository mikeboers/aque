import functools
import string

from . import *


def raise_(cls, *args):
    raise cls(*args)


class TestBasicIntegration(BrokerTestCase):

    def test_successful_builtins(self):

        f_list = self.queue.submit(list)
        f_upper = self.queue.submit(string.upper, 'hello')
        f_eval = self.queue.submit(eval, '100 + 20 + 3')

        self.worker.run_to_end()

        self.assertEqual(f_list.result(0), [])
        self.assertEqual(f_upper.result(0), 'HELLO')
        self.assertEqual(f_eval.result(0), 123)

    def test_errors(self):

        f_value = self.queue.submit(int, 'not an int')
        f_type = self.queue.submit(list, 123)
        f_runtime = self.queue.submit(raise_, RuntimeError)

        self.worker.run_to_end()

        self.assertRaises(ValueError, f_value.result, 0)
        self.assertRaises(TypeError, f_type.result, 0)
