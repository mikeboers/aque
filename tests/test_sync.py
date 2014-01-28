import functools

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


class TestOrder(TestCase):

    def test_branch(self):

        res = []

        a = Job(func=functools.partial(res.append, 'a'))
        b = Job(func=functools.partial(res.append, 'b'))
        c = Job(func=functools.partial(res.append, 'c'))

        a.children().extend((b, c))
        a.run()

        self.assertEqual(res, ['b', 'c', 'a'])

    def test_diamond(self):

        res = []

        a = Job(func=functools.partial(res.append, 'a'))
        b = Job(func=functools.partial(res.append, 'b'))
        c = Job(func=functools.partial(res.append, 'c'))
        d = Job(func=functools.partial(res.append, 'd'))

        a.children().extend((b, c))
        b.children().append(d)
        c.children().append(d)

        a.run()

        self.assertEqual(res, ['d', 'b', 'c', 'a'])


