from collections import Callable
from cPickle import PickleError
import cPickle as pickle
import json
import os
import pkg_resources
import select
import threading
import types


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
    except (TypeError, PickleError):
        pass

    try:
        return json.loads(encoded)
    except (TypeError, ValueError):
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


class WaitableEvent(object):

    def __init__(self):
        self._read_fd, self._write_fd = os.pipe()

    def wait(self, timeout=None):
        r, _, _ = select.select([self._read_fd], [], [], timeout)
        return bool(r)

    def is_set(self):
        return self.wait(0)

    def clear(self):
        if self.is_set():
            os.read(self._read_fd, 1)

    def set(self):
        if not self.is_set():
            os.write(self._write_fd, '1')

    def fileno(self):
        return self._read_fd

    def __del__(self):
        os.close(self._read_fd)
        os.close(self._write_fd)

