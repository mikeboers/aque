import contextlib
import threading

import psycopg2.pool
import psycopg2 as pg

import aque.utils as utils
from .base import Broker


class literal(str):

    def __conform__(self, quote):
        return self

    @classmethod
    def mro(cls):
        return (object, )

    def getquoted(self):
        return str(self)


class PostgresBroker(Broker):

    @classmethod
    def from_url(cls, parts):
        return cls(database=parts.path.strip('/').lower())

    def __init__(self, **kwargs):
        super(PostgresBroker, self).__init__()

        self._dbname = kwargs['database']
        self._pool = kwargs.pop('pool', None)
        if self._pool is None:
            self._pool = pg.pool.ThreadedConnectionPool(0, 10, **kwargs)

    @contextlib.contextmanager
    def _connect(self):
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        finally:
            self._pool.putconn(conn)

    @contextlib.contextmanager
    def _cursor(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                yield cur

    def init(self):
        with self._cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                status TEXT NOT NULL
            )''')

    def clear(self):
        with pg2.connect(database='postgres') as conn:
            conn.set_isolation_level(0)
            with conn.cursor() as cur:
                cur.execute('DROP DATABASE IF EXISTS %s' % self._dbname)
                cur.execute('CREATE DATABASE %s' % self._dbname)

    def create(self, prototype=None):
        with self._cursor() as cur:
            cur.execute('''INSERT INTO tasks (status) VALUES ('creating') RETURNING id''')
            tid = cur.fetchone()[0]
        if prototype:
            self.update(tid, prototype)
        return self.get_future(tid)

    def fetch(self, tid):
        raise NotImplementedError()

    def update(self, tid, data):
        pass

    def set_status_and_notify(self, tid, status):
        with self._cursor() as cur:
            cur.execute('''UPDATE tasks SET status = %s WHERE id = %s''', (status, tid))

    def iter_pending_tasks(self):
        with self._cursor() as cur:
            cur.execute('''SELECT * FROM tasks WHERE status = 'pending' ''')
            for res in cur:
                yield {'id': res[0]}


