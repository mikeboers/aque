from . import *

from aque.utils import *


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
        ):
            encoded = encode_if_required(v)
            decoded = decode_if_possible(encoded)
            self.assertEqual(v, decoded)

    def test_encode_dict(self):

        original = dict(int=1, str='hello', list=range(5))
        encoded = encode_values_when_required(original)

        self.assertEqual(encoded['int'], '1')
        self.assertEqual(encoded['str'], 'hello')
        self.assertEqual(encoded['list'], '[0,1,2,3,4]')

        decoded = decode_values_when_possible(encoded)
        self.assertEqual(original, decoded)
        
