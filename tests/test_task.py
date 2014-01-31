import functools

from . import *


class TestTaskBasics(TestCase):

    def test_children_mutability(self):
        parent = Task()
        child = Task()
        parent.children.append(child)
        self.assertEqual(len(parent.children), 1)
        self.assertIs(parent.children[0], child)

    def test_incomplete_results(self):
        task = Task()
        self.assertRaises(TaskIncomplete, task.result)

    def test_error_results(self):
        task = Task()
        task.error('dummy')
        self.assertRaises(TaskError, task.result)


class TestOrder(TestCase):

    def test_branch(self):

        res = []

        a = Task(func=functools.partial(res.append, 'a'))
        b = Task(func=functools.partial(res.append, 'b'))
        c = Task(func=functools.partial(res.append, 'c'))

        a.children.extend((b, c))
        a.run()

        self.assertEqual(res, ['b', 'c', 'a'])

    def test_diamond(self):

        res = []

        a = Task(func=functools.partial(res.append, 'a'))
        b = Task(func=functools.partial(res.append, 'b'))
        c = Task(func=functools.partial(res.append, 'c'))
        d = Task(func=functools.partial(res.append, 'd'))

        a.children.extend((b, c))
        b.children.append(d)
        c.children.append(d)

        a.run()

        self.assertEqual(res, ['d', 'b', 'c', 'a'])

    def test_loop(self):

        a = Task()
        b = Task()

        a.children.append(b)
        b.children.append(a)

        self.assertRaises(DependencyError, a.run)

    def test_looped_branches(self):

        a = Task()
        b = Task()
        c = Task()

        a.children.extend((b, c))
        b.children.append(c)
        c.children.append(b)

        self.assertRaises(DependencyError, a.run)
