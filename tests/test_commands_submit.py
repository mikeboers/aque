from . import *


class TestSubmitCommand(BrokerTestCase):

    def test_basics(self):

        msg = 'hello from ' + self.id()

        tid = int(self_check_output(['submit', 'echo', msg]).strip())

        full_human_status = self_check_output(['status'])
        self.assertTrue(re.search(r'\b%d\s+pending' % tid, full_human_status))

        one_human_status = self_check_output(['status', str(tid)])
        self.assertTrue(re.search(r'\b%d\s+pending' % tid, one_human_status))

        with capture_output() as (out, _):
            self_call(['status', '--csv', 'id,status'])
        full_csv_status = list(DictReader(out))
        self.assertTrue(len(full_csv_status) >= 1)
        self.assertTrue(any(int(t['id']) == tid and t['status'] == 'pending' for t in full_csv_status))

        with capture_output() as (out, _):
            self_call(['status', '--csv', 'id,status', str(tid)])
        one_csv_status = list(DictReader(out))
        self.assertTrue(len(one_csv_status) == 1)
        t = one_csv_status[0]
        self.assertEqual(int(t['id']), tid )
        self.assertEqual(t['status'], 'pending')

        self.worker.run_to_end()

        one_status = self_check_output(['status', str(tid)])
        self.assertTrue(re.search(r'\b%d\s+success' % tid, one_status   ))

        # Unfortunately we can't actually capture this message.
