import itertools
import logging
import pkg_resources

from aque.utils import decode_callable
import aque.handlers


log = logging.getLogger(__name__)


class DependencyError(RuntimeError):
    """Raised when job dependencies cannot be resolved."""

class JobIncomplete(RuntimeError):
    """Raised by :meth:`Job.result` when the job did not complete."""

class JobError(RuntimeError):
    """Raised by :meth:`Job.result` when the job errored without raised an exception."""


class Job(dict):

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
        """Retrieve the results of running this job.

        :returns: The result of running the job.
        :raises: :class:`JobIncomplete` if incomplete, 
            :class:`JobError` if the job errored,
            or any exception that the execution of the job may have raised.

        """

        if self.status == 'success':
            return self.get('result')

        elif self.status == 'error':
            exc = self.get('exception')
            if exc:
                raise exc
            message = '{} from {}'.format(self.get('error', 'unknown'), self.get('id', 'unknown'))
            type_ = self.get('error_type', JobError)
            if isinstance(type_, basestring):
                type_ = getattr(__builtins__, type_, JobError)
            raise type_(message)

        else:
            raise JobIncomplete('job is %s' % self.status)

    def error(self, message):
        """Signal that the job has errored while running."""
        self['status'] = 'error'
        self['error'] = message

    def complete(self, result=None):
        """Signal that the job has completed running."""
        self['status'] = 'success'
        self['result'] = result

    def dependencies(self):
        subs = self.setdefault('dependencies', [])
        for i, x in enumerate(subs):
            if not isinstance(x, Job):
                subs[i] = Job(x)
        return subs

    def children(self):
        subs = self.setdefault('children', [])
        for i, x in enumerate(subs):
            if not isinstance(x, Job):
                subs[i] = Job(x)
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
        for job in list(self._iter_linearized()):
            job._run()
        return self.setdefault('result', None)

    def _run(self):

        job_type = self.get('type', 'generic')
        handler = aque.handlers.registry.get(job_type, job_type)
        handler = decode_callable(handler)

        if handler is None:
            raise ValueError('no aque handler for type %r' % job_type)

        log.debug('handling job %r with %r' % (self['id'], handler))
        
        try:
            handler(self)
        except Exception as e:
            self['status'] = 'error'
            if e.args:
                self['error'] = e.args[0]
            self['error_type'] = e.__class__.__name__
            self['exception'] = e
            raise



