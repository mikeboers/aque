from __future__ import division

from collections import Callable
from cPickle import PickleError
import cPickle as pickle
import json
import logging
import os
import pkg_resources
import re
import threading
import types

log = logging.getLogger(__name__)


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

    if not isinstance(encoded, basestring):
        return encoded

    try:
        return pickle.loads(encoded)
    except PickleError:
        pass
    except (AttributeError, ImportError, TypeError):
        log.warning('pickle is no longer valid', exc_info=True)

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


def encode_callable(input_):
    if input_ is None:
        return None
    if isinstance(input_, basestring):
        return input_
    try:
        return '%s:%s' % (input_.__module__, input_.__name__)
    except AttributeError:
        raise TypeError('not a simple callable: %r' % input_)


def decode_callable(input_, entry_point_group=None):

    # 1. If it is callable, pass through.
    if isinstance(input_, Callable):
        return input_

    # 2. Try to unpickle it.
    try:
        return pickle.loads(input_)
    except (PickleError, TypeError):
        pass

    # 3. Try to parse it as "<module_list>:<attr_list>"
    try:
        module_name, attr_list = input_.split(':')
        module = __import__(module_name, fromlist=['.'])
        func = module
        for attr_name in attr_list.split('.'):
            func = getattr(func, attr_name)
        return func
    except (ValueError, ImportError, AttributeError):
        pass

    # 4. Try to look it up in the given entrypoint group.
    if entry_point_group:
        for ep in pkg_resources.iter_entry_points(entry_point_group, input_):
            return ep.load()

    # 5. Give up!.
    raise ValueError('could not decode callable from %r' % input_)




SI_PREFIXES = ('', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
def format_bytes(bytes):
    for prefix in SI_PREFIXES:
        if bytes < 1024:
            break
        bytes //= 1024
    else:
        bytes *= 1024
    return '%d%sB' % (bytes, prefix)


def parse_bytes(formatted):
    m = re.match(r'^(\d*\.?\d*)([kMGTPEZY]?)B?$', formatted)
    if m:
        return float(m.group(1)) * 1024 ** SI_PREFIXES.index(m.group(2))

