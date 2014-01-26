import itertools
import logging
import pkg_resources

from aque.utils import decode_callable
import aque.handlers


log = logging.getLogger(__name__)


class JobError(Exception):
    pass


class Job(dict):

    def __init__(self, *args, **kwargs):

        for x in itertools.chain(args, (kwargs, )):
            self.update(x)

        self.setdefaults(
            type='generic',
            status='pending',
        )

    def setdefaults(self, *args, **kwargs):
        res = {}
        for x in itertools.chain(args, (kwargs, )):
            for k, v in x.iteritems():
                res[k] = self.setdefault(k, v)
        return res

    def result(self):

        status = self.get('status')

        if status == 'pending':
            raise ValueError('job is pending')
        if status == 'success':
            return self.get('result')

        exc = self.get('exception')
        if exc:
            raise exc

        message = '{} from {}'.format(self.get('error', 'unknown error'), jid)
        type_ = self.get('error_type', JobError)
        if isinstance(type_, basestring):
            type_ = getattr(__builtins__, type_, JobError)
        raise type_(message)

    def error(self, message):
        self['status'] = 'error'
        self['error'] = message

    def success(self, result=None):
        self['status'] = 'success'
        self['result'] = result

    def dependencies(self):
        res = []
        for x in self.get('dependencies', ()):
            if not isinstance(x, Job):
                x = Job(x)
            res.append(x)
        return res

    def children(self):
        res = []
        for x in self.get('children', ()):
            if not isinstance(x, Job):
                x = Job(x)
            res.append(x)
        return res

    def run(self, default_id='main'):

        jid = self.setdefault('id', default_id)
        self['status'] == 'pending'

        for i, job in enumerate(self.dependencies()):
            job.run(default_id='{}.dep[{}]'.format(jid, i))
        for i, job in enumerate(self.children()):
            job.run(default_id='{}.child[{}]'.format(jid, i))

        try:
            self._run()
        except Exception as e:
            self['status'] = 'error'
            if e.args:
                self['error'] = e.args[0]
            self['error_type'] = e.__class__.__name__
            self['exception'] = e
            raise
        else:
            self['status'] = 'success'
            return self.setdefault('result', None)

    def _run(self):
        
        job_type = self.get('type', 'generic')
        handler = aque.handlers.registry.get(job_type, job_type)
        handler = decode_callable(handler)

        if handler is None:
            raise ValueError('no aque handler for type %r' % job_type)

        log.debug('handling job %r with %r' % (self['id'], handler))
        handler(self)



