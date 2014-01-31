from . import *


class TestSync(TestCase):

    def test_single_generic_task(self):

        task = Task(func=str, args=(123, ))
        res = task.run()

        self.assertEqual(res, '123')

    def test_map_reduce(self):

        square = lambda x: x * x
        add = lambda x, y: x + y

        children = [Task(func=square, args=(i, )) for i in xrange(1, 5)]
        task = Task(pattern='reduce_children', func=add, children=children)
        res = task.run()

        self.assertEqual(res, 1 + 4 + 9 + 16)
