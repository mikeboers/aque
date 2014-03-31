from . import *


class TestXargsCommand(BrokerTestCase):

    def test_basic_usage(self):

        path = os.path.join(self.sandbox, 'one.txt')
        out = self_check_output(['-v', 'xargs', '--', 'bash', '-c', 'echo $AQUE_TID >> "%s"' % path], stdin='1\n')
        tids = [int(x) for x in out.strip().split()]
        self.worker.run_to_end()
        self.assertEqual(open(path).read().strip(), str(tids[0]))

        path = os.path.join(self.sandbox, 'four.txt')
        out = self_check_output(['-v', 'xargs', '--', 'bash', '-c', 'echo $AQUE_TID >> "%s"' % path], stdin='1 2\n3 4\n')
        tids = [int(x) for x in out.strip().split()]
        self.assertEqual(len(tids), 2)
        self.worker.run_to_end()
        self.assertEqual(open(path).read().strip(), '\n'.join(str(x) for x in tids[:-1]))

    def test_word_counts(self):

        path = os.path.join(self.sandbox, 'one.txt')
        out = self_check_output(['-v', 'xargs', '-n1', '--', 'bash', '-c', 'echo $AQUE_TID >> "%s"' % path], stdin='1\n')
        tids = [int(x) for x in out.strip().split()]
        self.worker.run_to_end()
        self.assertEqual(open(path).read().strip(), str(tids[0]))

        path = os.path.join(self.sandbox, 'four.txt')
        out = self_check_output(['-v', 'xargs', '-n1', '--', 'bash', '-c', 'echo $AQUE_TID >> "%s"' % path], stdin='1 2\n3 4\n')
        tids = [int(x) for x in out.strip().split()]
        self.assertEqual(len(tids), 5)
        self.worker.run_to_end()
        self.assertEqual(open(path).read().strip(), '\n'.join(str(x) for x in tids[:-1]))

    def test_line_counts(self):

        path = os.path.join(self.sandbox, 'one.txt')
        out = self_check_output(['-v', 'xargs', '-L1', '--', 'bash', '-c', 'echo $AQUE_TID >> "%s"' % path], stdin='1\n')
        tids = [int(x) for x in out.strip().split()]
        self.worker.run_to_end()
        self.assertEqual(open(path).read().strip(), str(tids[0]))

        path = os.path.join(self.sandbox, 'four.txt')
        out = self_check_output(['-v', 'xargs', '-L1', '--', 'bash', '-c', 'echo $AQUE_TID >> "%s"' % path], stdin='1 2\n3 4\n')
        tids = [int(x) for x in out.strip().split()]
        self.assertEqual(len(tids), 3)
        self.worker.run_to_end()
        self.assertEqual(open(path).read().strip(), '\n'.join(str(x) for x in tids[:-1]))

