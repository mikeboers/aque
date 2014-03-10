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

    _fields = (
        'id',
        'status',
        'priority',
        'user',
        'group',
        'pattern',
        'func',
        'args',
        'kwargs',
    )

    @classmethod
    def from_url(cls, parts):
        return cls(database=parts.path.strip('/').lower())

    def __init__(self, **kwargs):
        super(PostgresBroker, self).__init__()

        self._kwargs = kwargs
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
                status TEXT,
                priority INTEGER,
                "user" TEXT,
                "group" TEXT,
                pattern TEXT,
                func TEXT,
                args TEXT,
                kwargs TEXT
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS dependencies (
                id SERIAL PRIMARY KEY,
                depender INTEGER references tasks(id),
                dependee INTEGER references tasks(id)
            )''')

    def clear(self):
        dbname = self._kwargs['database']
        kwargs = self._kwargs.copy()
        kwargs['database'] = 'postgres'
        with pg.connect(**kwargs) as conn:
            conn.set_isolation_level(0)
            with conn.cursor() as cur:
                cur.execute('DROP DATABASE IF EXISTS %s' % dbname)
                cur.execute('CREATE DATABASE %s' % dbname)

    def create(self, prototype=None):
        with self._cursor() as cur:
            cur.execute('''INSERT INTO tasks (status) VALUES ('creating') RETURNING id''')
            tid = cur.fetchone()[0]
        if prototype:
            self.update(tid, prototype)
        return self.get_future(tid)

    def fetch(self, tid):
        query = 'SELECT %s FROM tasks WHERE id = %%s' % ', '.join('"%s"' % name for name in self._fields)
        with self._cursor() as cur:
            cur.execute(query, (tid, ))
            row = cur.fetchone()
            task = dict(zip(self._fields, row))
            cur.execute('SELECT dependee FROM dependencies WHERE depender = %s', (tid, ))
            task['dependencies'] = [row[0] for row in cur]
        return task

    def update(self, tid, data):
        fields = []
        params = []
        for name in self._fields:
            try:
                value = data.pop(name)
            except KeyError:
                pass
            else:
                fields.append(name)
                params.append(utils.encode_if_required(value))

        deps = data.pop('dependencies', None)

        if data:
            raise ValueError('unexpected keys: %s' % ', '.join(sorted(data)))

        params.append(tid)
        query = 'UPDATE tasks SET %s WHERE id = %%s' % ', '.join('"%s" = %%s' % name for name in fields)
        with self._cursor() as cur:
            cur.execute(query, params)
            if deps is not None:
                cur.execute('DELETE FROM dependencies WHERE depender = %s', (tid, ))
                cur.executemany(
                    'INSERT INTO dependencies(depender, dependee) VALUES(%s, %s)',
                    [(tid, dep) for dep in deps]
                )

    def set_status_and_notify(self, tid, status):
        with self._cursor() as cur:
            cur.execute('''UPDATE tasks SET status = %s WHERE id = %s''', (status, tid))

    def iter_pending_tasks(self):
        with self._cursor() as cur:
            cur.execute('''SELECT * FROM tasks WHERE status = 'pending' ''')
            for res in cur:
                yield {'id': res[0]}


