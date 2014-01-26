from . import *


class TestSync(TestCase):

    def test_single_generic_job(self):

        job = Job(func=str, args=(123, ))
        job.run()

        self.assertEqual(job['result'], '123')
