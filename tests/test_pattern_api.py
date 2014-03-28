from . import *


def bad_pattern(broker, task):
    pass


class TestPatternAPI(BrokerTestCase):

    def test_missing_pattern(self):

        f = self.queue.submit_ex(tuple, pattern='not a pattern')
        self.worker.run_to_end()
        self.assertRaises(PatternMissingError, f.result)

    def test_bad_pattern(self):

        f = self.queue.submit_ex(tuple, pattern=bad_pattern)
        self.worker.run_to_end()
        self.assertRaises(PatternIncompleteError, f.result)
