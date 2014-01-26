import json
import cPickle as pickle
from cPickle import PickleError


def encode_if_required(value):
    """Serialize a value if it isn't a string.

    Will use JSON if it can, falling back to pickling.

    """
    if isinstance(value, basestring):
        return value
    else:
        try:
            return json.dumps(value, separators=(',', ':'))
        except TypeError:
            return pickle.dumps(value, protocol=-1)


def decode_if_possible(encoded):
    """Unserialize a value if possible.

    Tries pickle.dumps, then json.dumps, then returns the original

    """

    try:
        return pickle.loads(encoded)
    except PickleError:
        pass

    try:
        return json.loads(encoded)
    except ValueError:
        pass

    return encoded

def encode_values_when_required(input_):
    """JSON encode the values of a dictionary when they are not strings.

    :return: A new dictionary.

    """

    return dict((k, encode_if_required(v)) for k, v in input_.iteritems())

def decode_values_when_possible(input_):
    """JSON decode the values of a dictionary when they are valid JSON.

    :return: A new dictionary.

    """

    return dict((k, decode_if_possible(v)) for k, v in input_.iteritems())
