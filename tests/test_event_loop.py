from . import *


class TestTimers(TestCase):

    def test_timer(self):

        res = []
        one = functools.partial(res.append, 1)
        two = functools.partial(res.append, 2)
        def once():
            res.append(3)
            raise StopSelection()

        start_time = time.time()
        loop = EventLoop()
        loop.add_timer(0.010, one)
        loop.add_timer(0.010, once)
        loop.add_timer(0.015, two)

        loop.process(1)
        elapsed = time.time() - start_time
        self.assertGreater(elapsed, 0.01)
        self.assertLess(elapsed, 0.02)
        self.assertEqual(res, [1, 3])

        loop.process(1)
        elapsed = time.time() - start_time
        self.assertGreater(elapsed, 0.015)
        self.assertLess(elapsed, 0.02)
        self.assertEqual(res, [1, 3, 2])

        loop.process(1)
        elapsed = time.time() - start_time
        self.assertGreater(elapsed, 0.02)
        self.assertLess(elapsed, 0.025)
        self.assertEqual(res, [1, 3, 2, 1])

        loop.remove_timer(one)
        loop.process(1)
        elapsed = time.time() - start_time
        self.assertGreater(elapsed, 0.03)
        self.assertLess(elapsed, 0.45)
        self.assertEqual(res, [1, 3, 2, 1, 2])


