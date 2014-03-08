import functools

from . import *


class TestOrder(TestCase):

    def test_branch(self):

        res = []

        a = {'func': functools.partial(res.append, 'a')}
        b = {'func': functools.partial(res.append, 'b')}
        c = {'func': functools.partial(res.append, 'c')}

        a['dependencies'] = [b, c]
        
        execute(a)

        # TODO: This seems backwards.
        self.assertEqual(res, ['c', 'b', 'a'])

    def test_diamond(self):

        res = []

        a = {'func': functools.partial(res.append, 'a')}
        b = {'func': functools.partial(res.append, 'b')}
        c = {'func': functools.partial(res.append, 'c')}
        d = {'func': functools.partial(res.append, 'd')}

        a['dependencies'] = [b, c]
        b['dependencies'] = [d]
        c['dependencies'] = [d]

        execute(a)

        # TODO: This seems backwards.
        self.assertEqual(res, ['d', 'c', 'b', 'a'])

    def test_loop(self):
        a = {}
        b = {'dependencies': [a]}
        a['dependencies'] = [b]
        self.assertRaises(DependencyError, execute, a)

    def test_looped_branches(self):
        a = {}
        b = {}
        c = {}
        c['dependencies'] = [b]
        b['dependencies'] = [c]
        a['dependencies'] = [a, b]
        self.assertRaises(DependencyError, execute, a)
