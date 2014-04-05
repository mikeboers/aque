from . import *


class TestPatternAPI(BrokerTestCase):

    def test_missing_pattern(self):

        f = self.queue.submit_ex(tuple, pattern='not_a_pattern')
        self.worker.run_to_end()
        self.assertRaises(PatternMissingError, f.result, 0.1)
