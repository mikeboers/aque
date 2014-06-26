from __future__ import division

from collections import Callable, namedtuple
from cStringIO import StringIO
import cPickle
import pickle
import errno
import json
import logging
import os
import pkg_resources
import re
import subprocess
import sys
import threading
import types


log = logging.getLogger(__name__)


def debug(msg):
    if True:
        f = sys._getframe(1)
        print 'DEBUG: %s [%s:%d]' % (msg, f.f_globals.get('__file__', ''), f.f_lineno)


# There are some times we may want to get into the pickle-loading process to
# fix some known issues. The big one that we are facing is stdlib exceptions
# that have required arguments, don't set them to `args`, and don't re-implement
# __reduce__.
class SafeUnpickler(pickle.Unpickler):

    dispatch = pickle.Unpickler.dispatch.copy()

    def fixed_load_reduce(self):
        func = self.stack[-2]
        if issubclass(func, BaseException) and func.__reduce__ is BaseException.__reduce__:
            args = self.stack[-1]
            if not args:
                self.stack.pop()
                self.stack[-1] = func.__new__(func)
                return
        self.load_reduce()

    dispatch[pickle.REDUCE] = fixed_load_reduce


def safe_unpickle(input_):
    try:
        
        try:
            return cPickle.loads(str(input_))
        except TypeError as e:

            # The cPickle exception gives us the function and the arguments
            # that it called it with. If this seems to match the patterns that
            # the SafeUnpickler can deal with, then let it try.
            #
            # When an exception that does not set args is restored, we get:
            # TypeError('__init__() takes exactly 2 arguments (1 given)', <class '__main__.MyError'>, ())
            if len(e.args) == 3 and e.args[0].startswith('__init__()') and issubclass(e.args[1], BaseException) and not e.args[2]:
                return SafeUnpickler(StringIO(str(input_))).load()
            else:
                raise

    except (cPickle.PickleError, pickle.PickleError) as e:
        raise
    except (TypeError, ValueError, ImportError) as e:
        log.exception('pickle is unpicklable in current environment')
        return None


def encode_callable(input_):

    if input_ is None:
        return None

    if isinstance(input_, basestring):
        return input_

    # Methods.
    try:
        return '%s:%s.%s' % (input_.__module__, input_.im_class.__name__, input_.__name__)
    except AttributeError:
        pass

    # Functions.
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
        return cPickle.loads(input_)
    except (cPickle.PickleError, TypeError):
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


_mounts = []
Mount = namedtuple('Mount', ['device', 'path', 'type', 'flags'])


def mounts():
    """Get list of mounted filesystems.

    Each mount is a namedtuple with fields: device, path, type, and flags.

    """
    if not _mounts:
        raw = subprocess.check_output(['mount'])
        if sys.platform == 'darwin':
            _mounts.extend(_parse_darwin_mounts(raw))
        elif sys.platform.startswith('linux'):
            _mounts.extend(_parse_linux_mounts(raw))
        else:
            log.warning('cannot parse mounts for %s' % sys.platform)
    return list(_mounts)


def _parse_darwin_mounts(raw):
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r'^(.+?) on (/.*?) \(([^,]+)(?:, )?(.*?)\)$', line)
        if not m:
            log.warning('could not parse mount line %r' % line)
            continue
        device, path, type_, flags = m.groups()
        yield Mount(device, path, type_, frozenset(flags.split(', ')))


def _parse_linux_mounts(raw):
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r'^(.+?) on (/.*?) type (.+?) \((.+?)\)$', line)
        if not m:
            log.warning('could not parse mount line %r' % line)
            continue
        device, path, type_, flags = m.groups()
        yield Mount(device, path, type_, frozenset(flags.split(',')))


def get_mount(path, _mounts=None):
    """Get the mount corresponding to the given path."""
    path = os.path.abspath(path)
    for mount in sorted(mounts() if _mounts is None else _mounts, key=lambda m: len(m.path), reverse=True):
        if path.startswith(mount.path) and (mount.path.endswith('/') or path[len(mount.path):len(mount.path)+1] in ('/', '')):
            return mount


def paths_from_args(args):
    res = []
    for arg in args:
        path = os.path.abspath(arg)
        if os.path.exists(path):
            res.append(path)
    return res

