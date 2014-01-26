import json


def encode_if_required(value):
    """JSON encode if not a string."""
    if isinstance(value, basestring):
        return value
    else:
        return json.dumps(value, separators=(',', ':'))

def decode_if_possible(encoded):
    """JSON decode if it is valid JSON, otherwise return original."""
    try:
        return json.loads(encoded)
    except ValueError:
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
