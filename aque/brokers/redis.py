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

    def clear(self):
        existing = self._redis.keys(self._name + ':*')
        if existing:
            self._redis.delete(*existing)

    def create(self, prototype=None):
        tid = self._key('task:{}', self._redis.incr(self._key('task_counter')))
        if prototype:
            self.update(tid, prototype)
        self._redis.rpush(self._key('all_tasks'), tid)
        return self.get_future(tid)

    def fetch(self, tid):
        task = utils.decode_values_when_possible(self._redis.hgetall(tid))
        task['id'] = tid
        return task

    def update(self, tid, data):
        self._redis.hmset(tid, utils.encode_values_when_required(data))

    ## Medium-level API

    def _key(self, format, *args, **kwargs):
        if kwargs.pop('_db', None):
            return ('{}@{}:' + format).format(self._name, self._db, *args)
        else:
            return ('{}:' + format).format(self._name, *args)

    def set_status_and_notify(self, tid, status):
        self.update(tid, {'status': status})
        self._redis.publish(self._key('status_changes', _db=True), '%s %s' % (tid, status))
        self._redis.publish(self._key('%s:status_change', tid, _db=True), status)

    def mark_as_pending(self, tid):
        super(RedisBroker, self).mark_as_pending(tid)
        self._redis.sadd(self._key('pending_tasks'), tid)

    def mark_as_success(self, tid, result):
        super(RedisBroker, self).mark_as_success(tid, result)
        self._redis.srem(self._key('pending_tasks'), tid)

    def mark_as_error(self, tid, exception):
        super(RedisBroker, self).mark_as_error(tid, exception)
        self._redis.srem(self._key('pending_tasks'), tid)

    def iter_pending_tasks(self):
        for tid in self._redis.smembers(self._key('pending_tasks')):
            yield self.fetch(tid)



