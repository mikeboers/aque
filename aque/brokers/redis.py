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

    ## Low-level API

    def get(self, tid, key):
        return utils.decode_if_possible(self._redis.hget(tid, key))

    def getall(self, tid):
        return utils.decode_values_when_possible(self._redis.hgetall(tid))

    def set(self, tid, key, value):
        self._redis.hset(tid, key, utils.encode_if_required(value))

    def setmany(self, tid, data):
        self._redis.hmset(tid, utils.encode_values_when_required(data))

    ## Medium-level API

    def _format_key(self, format, *args, **kwargs):
        if kwargs.pop('_db', None):
            return ('{}@{}:' + format).format(self._name, self._db, *args)
        else:
            return ('{}:' + format).format(self._name, *args)

    def new_task_id(self):
        return self._format_key('task:{}', self._redis.incr(self._format_key('task_counter')))


    ## High-level API

    def set_status(self, tid, status):
        super(RedisBroker, self).set_status(tid, status)
        self._redis.publish(self._format_key('status_changes'), '%s %s' % (tid, status))

    def mark_as_pending(self, tid):
        """Setup the task to run when able."""
        super(RedisBroker, self).mark_as_pending(self)
        self._redis.rpush(self._format_key('pending_tasks'), tid)

    def get_pending_task_ids(self):
        return set(self._redis.lrange(self._format_key('pending_tasks'), 0, -1))


