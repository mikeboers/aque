import collections
import itertools
import logging
import pkg_resources

from aque.utils import decode_callable
import aque.patterns


log = logging.getLogger(__name__)


class DependencyError(RuntimeError):
    """Raised when task dependencies cannot be resolved."""

class TaskIncomplete(RuntimeError):
    """Raised by :meth:`Task.result` when the task did not complete."""

class TaskError(RuntimeError):
    """Raised by :meth:`Task.result` when the task errored without raised an exception."""


class BaseProperty(object):

    def objstore(self, obj):
        raise NotImplementedError()

    def __init__(self, name, default=None):
        self.name = name
        self.default = default

    def __get__(self, obj, cls):
        if obj is None:
            return self
        try:
            return self.objstore(obj)[self.name]
        except KeyError:
            if self.default:
                return self.objstore(obj).setdefault(self.name, self.default())
            raise AttributeError(self.name)

    def __set__(self, obj, value):
        self.objstore(obj)[self.name] = value


class staticproperty(BaseProperty):

    def objstore(self, obj):
        return obj._static


class dynamicproperty(BaseProperty):

    def objstore(self, obj):
        return obj._dynamic



class Task(collections.MutableMapping):

    pattern = staticproperty('pattern')
    func = staticproperty('func')
    args = staticproperty('args')
    kwargs = staticproperty('kwargs')

    children = staticproperty('children', default=list)
    dependencies = staticproperty('dependencies', default=list)

    status = dynamicproperty('status')


    def __init__(self, func=None, args=None, kwargs=None, **extra):

        self.id = None

        self._static = {}
        self._dynamic = {}

        self.pattern = 'generic'        
        self.func = func
        self.args = list(args or ())
        self.kwargs = dict(kwargs or {})

        self.status = 'pending'

        for k, v in extra.iteritems():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                self[k] = v

    def __getitem__(self, key):
        try:
            return self._dynamic[key]
        except KeyError:
            return self._static[key]

    def __setitem__(self, key, value):
        if self._dynamic:
            self._dynamic[key] = value
        else:
            self._static[key] = value

    def __delitem__(self, key):
        try:
            del self._dynamic[key]
        except KeyError:
            if key in self._static:
                raise RuntimeError("can't del from Task._static")
            raise

    def __hash__(self):
        return id(self)

    def __iter__(self):
        return iter(set(self._static).union(self._dynamic))

    def __len__(self):
        return len(set(self._static).union(self._dynamic))

    def result(self, strict=True):
        """Retrieve the results of running this task.

        :returns: The result of running the task.
        :raises: :class:`TaskIncomplete` if incomplete, 
            :class:`TaskError` if the task errored,
            or any exception that the execution of the task may have raised.

        """

        if self.status == 'success':
            return self._dynamic.get('result')

        elif not strict:
            return

        elif self.status == 'error':
            exc = self._dynamic.get('exception')
            if exc:
                raise exc
            message = '{} from {}'.format(self._dynamic.get('error', 'unknown'), self.id)
            type_ = self._dynamic.get('error_type', TaskError)
            if isinstance(type_, basestring):
                type_ = getattr(__builtins__, type_, TaskError)
            raise type_(message)

        else:
            raise TaskIncomplete('task is %s' % self.status)

    def error(self, message):
        """Signal that the task has errored while running.

        :returns: A :class:`TaskError` that can be raised.

        """

        self.status = 'error'
        self._dynamic['error'] = message

        # TODO: publish on redis
        return TaskError(message)

    def complete(self, result=None):
        """Signal that the task has completed running."""

        self.status = 'success'
        self._dynamic['result'] = result
        # TODO: publish on redis

    def assert_graph_ids(self, base=None, index=1, _visited=None):
        """Make sure the entire DAG rooted at this point has IDs."""

        _visited = _visited or set()
        if self in _visited:
            return
        _visited.add(self)

        if self.id is None:
            self.id = (base + '.' if base else '') + str(index)

        for i, dep in enumerate(self.dependencies):
            dep.assert_graph_ids(base, index+1, _visited)
        for i, child in enumerate(self.children):
            child.assert_graph_ids(self.id, 1, _visited)

    def _iter_linearized(self, _visited=None):

        _visited = _visited or set()
        if (self, 'incomplete') in _visited:
            raise DependencyError('cycle in dependencies with %r' % self.id)
        if self in _visited:
            return

        _visited.add((self, 'incomplete'))
        _visited.add(self)

        for x in self.dependencies:
            for y in x._iter_linearized(_visited):
                yield y
        for x in self.children:
            for y in x._iter_linearized(_visited):
                yield y

        _visited.remove((self, 'incomplete'))
        yield self

    def run(self):
        self.assert_graph_ids()
        for task in list(self._iter_linearized()):
            task._run()
        return self._dynamic.setdefault('result', None)

    def _run(self):
        """Find the pattern handler, call it, and catch errors."""

        pattern_name = self.pattern
        pattern_func = aque.patterns.registry.get(pattern_name, pattern_name)
        pattern_func = decode_callable(pattern_func)

        if pattern_func is None:
            raise ValueError('no aque pattern %r' % pattern_name)

        # log.debug('handling task %r with %r' % (self['id'], pattern))
        
        try:
            pattern_func(self)
        except Exception as e:
            self.status = 'error'
            if e.args:
                self._dynamic['error'] = e.args[0]
            self._dynamic['error_type'] = '{}.{}'.format(
                e.__class__.__module__,
                e.__class__.__name__,
            )
            self._dynamic['exception'] = e
            raise


    def progress(self, value, max=None, message=None):
        """Notify the web UI of progress."""

        if value is not None:
            self['progress'] = value
        if max is not None:
            self['progress_max'] = max
        if message is not None:
            self['progress_message'] = message

        # TODO: publish to some channel

    def ping(self):
        """Notify the other workers that this task is not dead."""
        # TODO: do something.
    



