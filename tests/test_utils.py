from . import *

from aque.utils import *


def func_to_encode():
    pass


class TestEncodingUtils(TestCase):
    
    def test_encoding(self):

        self.assertEqual(encode_if_required('hello'), 'hello')
        self.assertEqual(encode_if_required(123), '123')
        self.assertEqual(encode_if_required(range(5)), '[0,1,2,3,4]')
        self.assertEqual(encode_if_required(('a', 'b', 'c')), '["a","b","c"]')

    def test_roundtrip(self):
        for v in (
            'hello',
            123,
            range(5),
            list('abc'),
            dict(int=1, str='hello', list=range(5)),
            func_to_encode,
        ):
            encoded = encode_if_required(v)
            decoded = decode_if_possible(encoded)
            self.assertEqual(v, decoded)

    def test_encode_dict(self):

        original = dict(int=1, str='hello', list=range(5), func=func_to_encode)
        encoded = encode_values_when_required(original)

        self.assertEqual(encoded['int'], '1')
        self.assertEqual(encoded['str'], 'hello')
        self.assertEqual(encoded['list'], '[0,1,2,3,4]')
        # Can't really check the function here.

        decoded = decode_values_when_possible(encoded)
        self.assertEqual(original, decoded)


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
        
