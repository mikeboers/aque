from collections import Callable
from cPickle import PickleError
import cPickle as pickle
import json
import pkg_resources


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


def decode_callable(input_, entrypoint_group='aque_patterns'):

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
        module = __import__(module_name, from_list=['.'])
        func = module
        for attr_name in attr_list.split('.'):
            func = getattr(func, attr_name)
        return func
    except (ValueError, ImportError, AttributeError):
        pass

    # 4. Try to look it up in the given entrypoint group.
    for ep in pkg_resources.iter_entry_points(entrypoint_group, input_):
        return ep.load()

    # 5. Give up!.
    raise ValueError('could not decode callable from %r' % input_)


def eval_expr_or_func(src, globals_, locals_=None, filename=None):

    if filename is None:
        filename = '<string:%s>' % (src.encode('string-escape'))

    lines = src.strip().splitlines()
    if len(lines) > 1:
        # Surely I can create a function object directly with a compiled code
        # object, but I couldn't quit figure it out in the time that I allowed.
        # Ergo, we are evalling strings. Sorry.
        src = 'def __expr__():\n' + '\n'.join('\t' + line for line in lines)
        locals_ = locals_ if locals_ is not None else {}
        code = compile(src, filename, 'exec')
        eval(code, globals_, locals_)
        return locals_['__expr__']()
    else:
        code = compile(lines[0], filename, 'eval')
        return eval(code, globals_)


class ExprDict(dict):

    def __getitem__(self, key):

        try:
            meth = super(ExprDict, self).__getitem__(key + '.meth')
        except KeyError:
            pass
        else:
            return meth(self)

        try:
            func = super(ExprDict, self).__getitem__(key + '.func')
        except KeyError:
            pass
        else:
            return func()

        try:
            expr = super(ExprDict, self).__getitem__(key + '.expr')
        except KeyError:
            pass
        else:
            return eval_expr_or_func(expr, self, {}, key + '.expr')

        return super(ExprDict, self).__getitem__(key)
