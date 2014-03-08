from . import *


class TestSync(TestCase):

    def test_single_generic_task(self):

        task = {'func': str, 'args': (123, )}
        res = execute(task)

        self.assertEqual(res, '123')

    def test_map_reduce(self):

        square = lambda x: x * x
        add = lambda x, y: x + y

        dependencies = [{'func': square, 'args': (i, )} for i in xrange(1, 5)]
        task = {'pattern': 'reduce_children', 'func': add, 'dependencies': dependencies}
        res = execute(task)

        self.assertEqual(res, 1 + 4 + 9 + 16)
