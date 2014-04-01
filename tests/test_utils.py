from . import *

from aque.utils import *


class TestBytesFormatting(TestCase):

    def test_format_bytes(self):
        self.assertEqual(format_bytes(0), '0B')
        self.assertEqual(format_bytes(1), '1B')
        self.assertEqual(format_bytes(1023), '1023B')
        self.assertEqual(format_bytes(1024), '1kB')
        self.assertEqual(format_bytes(1024**2), '1MB')
        self.assertEqual(format_bytes(1024**3), '1GB')
        self.assertEqual(format_bytes(1024**4), '1TB')
        self.assertEqual(format_bytes(1024**5), '1PB')
        self.assertEqual(format_bytes(1024**6), '1EB')
        self.assertEqual(format_bytes(1024**7), '1ZB')
        self.assertEqual(format_bytes(1024**8), '1YB')
        self.assertEqual(format_bytes(1024**9), '1024YB')

    def test_round_trip(self):

        for x in 0, 1, 1023, 1024, 1024**2, 1024**3:
            y = format_bytes(x)
            z = parse_bytes(y)
            self.assertEqual(x, z)
        
