import functools

from . import *


class TestJobBasics(TestCase):

    def test_children_mutability(self):
        parent = Job()
        child = Job()
        parent.children().append(child)
        self.assertEqual(len(parent.children()), 1)
        self.assertIs(parent.children()[0], child)

    def test_incomplete_results(self):
        job = Job()
        self.assertRaises(JobIncomplete, job.result)

    def test_error_results(self):
        job = Job()
        job.error('dummy')
        self.assertRaises(JobError, job.result)


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

    def test_loop(self):

        a = Job()
        b = Job()

        a.children().append(b)
        b.children().append(a)

        self.assertRaises(DependencyError, a.run)

    def test_looped_branches(self):

        a = Job()
        b = Job()
        c = Job()

        a.children().extend((b, c))
        b.children().append(c)
        c.children().append(b)

        self.assertRaises(DependencyError, a.run)
