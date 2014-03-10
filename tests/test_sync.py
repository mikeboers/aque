from . import *

def ex_add(a, b):
    return a + b

def ex_square(x):
    return x * x


class TestSync(TestCase):

    def test_single_generic_task(self):
        task = {'func': str, 'args': (123, )}
        res = execute(task)
        self.assertEqual(res, '123')

    def test_map_reduce(self):
        dependencies = [{'func': ex_square, 'args': (i, )} for i in xrange(1, 5)]
        task = {'pattern': 'reduce', 'func': ex_add, 'dependencies': dependencies}
        res = execute(task)
        self.assertEqual(res, 1 + 4 + 9 + 16)
