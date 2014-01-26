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

    def _raise_if_errored(self, jid=None):

        if self.get('status') != 'error':
            return

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
        self['error'] = str(message)
        raise JobError(message)

    def success(self, result=None):
        self['status'] = 'success'
        self['result'] = result

    def run(self, default_id='main'):

        jid = self.setdefault('id', default_id)
        self._raise_if_errored(jid)

        if self['status'] == 'pending':

            for i, job in enumerate(self.get('dependencies', ())):
                job.run(default_id='{}.dep[{}]'.format(jid, i))
            for i, job in enumerate(self.get('children', ())):
                job.run(default_id='{}.child[{}]'.format(jid, i))

            try:
                self._handle()
            except Exception as e:
                self['status'] = 'error'
                self['exception'] = e
                raise
            else:
                self['status'] = 'success'

        if self['status'] == 'success':
            return self.get('result')

    def _handle(self):

        type_ = self.get('type', 'generic')

        handler = aque.handlers.registry.get(type_)
        handler = decode_callable(handler)

        if handler is None:
            for ep in pkg_resources.iter_entry_points('aque_handlers', type_):
                handler = ep.load()
                break

        if handler is None:
            raise ValueError('no aque handler for type %r' % type_)

        log.debug('handling job %r with %r' % (self['id'], handler))
        handler(self)



