from . import *


class TestSync(TestCase):

    def test_single_generic_job(self):

        job = Job(func=str, args=(123, ))
        res = job.run()

        self.assertEqual(res, '123')

    def test_map_reduce(self):

        square = lambda x: x * x
        add = lambda x, y: x + y

        children = [Job(func=square, args=(i, )) for i in xrange(1, 5)]
        job = Job(type='reduce_children', func=add, children=children)
        res = job.run()

        self.assertEqual(res, 1 + 4 + 9 + 16)
