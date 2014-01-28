from . import *


class TestJobs(TestCase):


    def test_children_identity(self):
        job = Job()
        children = job.children()
        self.assertIs(children, job.children())

    def test_children_mutability(self):
        parent = Job()
        child = Job()
        parent.children().append(child)
        self.assertEqual(len(parent.children()), 1)
        self.assertIs(parent.children()[0], child)
