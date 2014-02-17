from urlparse import urlsplit

from redis import Redis

import aque.utils as utils


class Broker(object):
    """Handles all negotiation between the client (e.g. Python) and server.

    This initial broker backs onto Redis.

    """

    @classmethod
    def from_url(cls, url):
        url = urlsplit(url)
        if url.scheme != 'redis':
            raise ValueError('only supported broker is "redis://"')
        redis = Redis(
            url.hostname or 'localhost',
            url.port or 6379,
            int(url.path.strip('/')) if url.path else 0,
        )
        return cls(url.fragment or 'aque', redis)

    def __init__(self, name='aque', redis=None):
        self._redis = redis or Redis()
        self._name = name
        self._db = self._redis.connection_pool.connection_kwargs['db']

    def _format_key(self, format, *args, **kwargs):
        if kwargs.pop('_db', None):
            return ('{}@{}:' + format).format(self._name, self._db, *args)
        else:
            return ('{}:' + format).format(self._name, *args)

    def new_task_id(self):
        return self._format_key('task:{}', self._redis.incr(self._format_key('task_counter')))

    def get(self, tid, key):
        return utils.decode_if_possible(self._redis.hget(tid, key))

    def getall(self, tid):
        return utils.decode_values_when_possible(self._redis.hgetall(tid))

    def set(self, tid, key, value):
        self._redis.hset(tid, key, utils.encode_if_required(value))

    def setmany(self, tid, data):
        self._redis.hmset(tid, utils.encode_values_when_required(data))

    def set_status(self, tid, status):
        """Set status and publish to workers."""
        pass

    def mark_as_complete(self, tid, result):
        """Store a result and set the status to "complete"."""
        pass

    def mark_as_error(self, tid, exc):
        """Store an error and set the status to "error"."""
        pass

    def wait_for(self, tid, timeout=None):
        """Wait for a task to complete (or error).

        :param float timeout: seconds to block; blocks forever if is ``None``.
        :returns: result from the task.
        :raises: errors from the task, or :exception:`Timeout`.

        """
        pass

