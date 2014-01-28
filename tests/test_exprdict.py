from . import *


class TestExprDict(TestCase):

    def test_basics(self):

        x = ExprDict({
            'a': 1,
            'b.expr': '1 + 1',
            'c.expr': '# comment to force multiple lines\nreturn 1 + 2',
            'd.func': lambda: 4,
            'e.meth': lambda j: j['e.ret'],
            'e.ret': 5,
        })

        self.assertEqual(x['a'], 1)
        self.assertEqual(x['b'], 2)
        self.assertEqual(x['c'], 3)
        self.assertEqual(x['d'], 4)
        self.assertEqual(x['e'], 5)
