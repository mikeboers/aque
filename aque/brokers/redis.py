from __future__ import absolute_import

from urlparse import urlsplit

from redis import Redis

import aque.utils as utils
from .base import Broker


class RedisBroker(Broker):
    """Primary asynchonous :class:`.Broker` that backs onto Redis."""

    def __init__(self, name, redis):
        super(RedisBroker, self).__init__()
        self._name = name
        self._redis = redis
        self._db = self._redis.connection_pool.connection_kwargs['db']

    def create(self, prototype=None):
        tid = self._format_key('task:{}', self._redis.incr(self._format_key('task_counter')))
        if prototype:
            self.update(tid, prototype)
        return self.get_future(tid)

    def fetch(self, tid):
        task = utils.decode_values_when_possible(self._redis.hgetall(tid))
        task['id'] = tid
        return task

    def update(self, tid, data):
        self._redis.hmset(tid, utils.encode_values_when_required(data))

    ## Medium-level API

    def _format_key(self, format, *args, **kwargs):
        if kwargs.pop('_db', None):
            return ('{}@{}:' + format).format(self._name, self._db, *args)
        else:
            return ('{}:' + format).format(self._name, *args)

    def set_status_and_notify(self, tid, status):
        self.update(tid, {'status': status})
        self._redis.publish(self._format_key('status_changes'), '%s %s' % (tid, status))

    def mark_as_pending(self, tid, top_level=True):
        """Setup the task to run when able."""
        super(RedisBroker, self).mark_as_pending(self)
        if top_level:
            self._redis.rpush(self._format_key('pending_tasks'), tid)

    def iter_pending_tasks(self):
        for tid in set(self._redis.lrange(self._format_key('pending_tasks'), 0, -1)):
            yield self.fetch(tid)




