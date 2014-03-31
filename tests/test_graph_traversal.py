import functools

from . import *


def graph_order(task):
    broker = current_broker()
    res = []
    for dep_id in task.get('dependencies', ()):
        dep = broker.fetch(dep_id)
        for x in dep.get('result', ()):
            if x not in res:
                res.append(x)
    res.append(task.get('name', task['id']))
    return res


class TestGraphTraversal(BrokerTestCase):

    def test_branch(self):

        a = {'pattern': graph_order, 'name': 'a'}
        b = {'pattern': graph_order, 'name': 'b'}
        c = {'pattern': graph_order, 'name': 'c'}
        a['dependencies'] = [b, c]
        
        f = self.queue.submit_ex(**a)
        self.worker.run_to_end()
        res = f.result(0.1)

        self.assertEqual(res, ['b', 'c', 'a'])

    def test_diamond(self):

        a = {'pattern': graph_order, 'name': 'a'}
        b = {'pattern': graph_order, 'name': 'b'}
        c = {'pattern': graph_order, 'name': 'c'}
        d = {'pattern': graph_order, 'name': 'd'}

        a['dependencies'] = [b, c]
        b['dependencies'] = [d]
        c['dependencies'] = [d]

        f = self.queue.submit_ex(**a)
        self.worker.run_to_end()
        res = f.result(0.1)
        
        self.assertEqual(res, ['d', 'b', 'c', 'a'])

    def test_loop(self):
        a = {}
        b = {'dependencies': [a]}
        a['dependencies'] = [b]
        self.assertRaises(DependencyResolutionError, execute, a)

    def test_looped_branches(self):
        a = {}
        b = {}
        c = {}
        c['dependencies'] = [b]
        b['dependencies'] = [c]
        a['dependencies'] = [a, b]
        self.assertRaises(DependencyResolutionError, execute, a)


class TestGraphFlattening(BrokerTestCase):

    def test_simple_flattening(self):

        a = dict(name='a')
        b = dict(name='b')
        c = dict(name='c')
        d = dict(name='d')
        a['dependencies'] = [b, c]

        flattened = list(self.queue._flatten_prototypes([a, d]))
        self.assertEqual(flattened, [b, c, a, d])

    def test_loop(self):
        a = {}
        b = {'dependencies': [a]}
        a['dependencies'] = [b]
        self.assertRaises(DependencyResolutionError, list, self.queue._flatten_prototypes([a], {}))

    def test_looped_branches(self):
        a = {}
        b = {}
        c = {}
        c['dependencies'] = [b]
        b['dependencies'] = [c]
        a['dependencies'] = [a, b]
        self.assertRaises(DependencyResolutionError, list, self.queue._flatten_prototypes([a], {}))

