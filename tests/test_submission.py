import functools

from . import *


class TestOrder(TestCase):

    def test_branch(self):

        res = []

        a = {'func': functools.partial(res.append, 'a')}
        b = {'func': functools.partial(res.append, 'b')}
        c = {'func': functools.partial(res.append, 'c')}

        a['children'] = [b, c]
        
        execute(a)

        self.assertEqual(res, ['b', 'c', 'a'])

    def test_diamond(self):

        res = []

        a = {'func': functools.partial(res.append, 'a')}
        b = {'func': functools.partial(res.append, 'b')}
        c = {'func': functools.partial(res.append, 'c')}
        d = {'func': functools.partial(res.append, 'd')}

        a['children'] = [b, c]
        b['children'] = [d]
        c['children'] = [d]

        execute(a)

        self.assertEqual(res, ['d', 'b', 'c', 'a'])


    def test_loop(self):

        a = {}
        b = {'children': [a]}
        a['children'] = [b]

        self.assertRaises(DependencyError, execute, a)


    def test_looped_branches(self):

        a = {}
        b = {}
        c = {}

        c['children'] = [b]
        b['children'] = [c]
        a['children'] = [a, b]

        self.assertRaises(DependencyError, execute, a)
