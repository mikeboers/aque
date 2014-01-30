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


class Task(dict):

    def __init__(self, *args, **kwargs):

        for x in itertools.chain(args, (kwargs, )):
            self.update(x)

        self.setdefaults(
            type='generic',
            status='pending',
        )

    def __hash__(self):
        return id(self)


    def setdefaults(self, *args, **kwargs):
        res = {}
        for x in itertools.chain(args, (kwargs, )):
            for k, v in x.iteritems():
                res[k] = self.setdefault(k, v)
        return res

    @property
    def status(self):
        return self.setdefault('status', 'pending')

    def result(self):
        """Retrieve the results of running this task.

        :returns: The result of running the task.
        :raises: :class:`TaskIncomplete` if incomplete, 
            :class:`TaskError` if the task errored,
            or any exception that the execution of the task may have raised.

        """

        if self.status == 'success':
            return self.get('result')

        elif self.status == 'error':
            exc = self.get('exception')
            if exc:
                raise exc
            message = '{} from {}'.format(self.get('error', 'unknown'), self.get('id', 'unknown'))
            type_ = self.get('error_type', TaskError)
            if isinstance(type_, basestring):
                type_ = getattr(__builtins__, type_, TaskError)
            raise type_(message)

        else:
            raise TaskIncomplete('task is %s' % self.status)

    def error(self, message):
        """Signal that the task has errored while running."""
        self['status'] = 'error'
        self['error'] = message

    def complete(self, result=None):
        """Signal that the task has completed running."""
        self['status'] = 'success'
        self['result'] = result

    def dependencies(self):
        subs = self.setdefault('dependencies', [])
        for i, x in enumerate(subs):
            if not isinstance(x, Task):
                subs[i] = Task(x)
        return subs

    def children(self):
        subs = self.setdefault('children', [])
        for i, x in enumerate(subs):
            if not isinstance(x, Task):
                subs[i] = Task(x)
        return subs

    def assert_graph_ids(self, base=None, index=1, _visited=None):
        """Make sure the entire DAG rooted at this point has IDs."""

        _visited = _visited or set()
        if self in _visited:
            return
        _visited.add(self)

        jid = (base + '.' if base else '') + str(index)
        jid = self.setdefault('id', jid)

        for i, dep in enumerate(self.dependencies()):
            dep.assert_graph_ids(base, index+1, _visited)
        for i, child in enumerate(self.children()):
            child.assert_graph_ids(jid, 1, _visited)

    def _iter_linearized(self, _visited=None):

        _visited = _visited or set()
        if (self, 'incomplete') in _visited:
            raise DependencyError('cycle in dependencies with %r' % self.get('id'))
        if self in _visited:
            return

        _visited.add((self, 'incomplete'))
        _visited.add(self)

        for x in self.dependencies():
            for y in x._iter_linearized(_visited):
                yield y
        for x in self.children():
            for y in x._iter_linearized(_visited):
                yield y

        _visited.remove((self, 'incomplete'))
        yield self

    def run(self):
        self.assert_graph_ids()
        for task in list(self._iter_linearized()):
            task._run()
        return self.setdefault('result', None)

    def _run(self):

        task_type = self.get('type', 'generic')
        pattern = aque.patterns.registry.get(task_type, task_type)
        pattern = decode_callable(pattern)

        if pattern is None:
            raise ValueError('no aque pattern for type %r' % task_type)

        log.debug('handling task %r with %r' % (self['id'], pattern))
        
        try:
            pattern(self)
        except Exception as e:
            self['status'] = 'error'
            if e.args:
                self['error'] = e.args[0]
            self['error_type'] = e.__class__.__name__
            self['exception'] = e
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
    



