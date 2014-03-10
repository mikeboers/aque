import contextlib
import threading

import psycopg2.pool
import psycopg2 as pg

import aque.utils as utils
from .base import Broker


class PostgresBroker(Broker):

    @classmethod
    def from_url(cls, parts):
        return cls(database=parts.path.strip('/').lower())

    def __init__(self, **kwargs):
        super(PostgresBroker, self).__init__()

        self._kwargs = kwargs
        self._pool = kwargs.pop('pool', None)
        if self._pool is None:
            self._pool = pg.pool.ThreadedConnectionPool(0, 4, **kwargs)

        self.init()

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
                status TEXT NOT NULL DEFAULT 'creating',
                priority INTEGER NOT NULL DEFAULT 1000,
                "user" TEXT,
                "group" TEXT,
                name TEXT,
                pattern TEXT,
                func BYTEA,
                args BYTEA,
                kwargs BYTEA,
                result BYTEA
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS dependencies (
                depender INTEGER NOT NULL references tasks(id),
                dependee INTEGER NOT NULL references tasks(id),
                UNIQUE(depender, dependee)
            )''')

            # Determine what fields actually exist.
            cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name = 'tasks' ''')
            self._task_fields = tuple(row[0] for row in cur)


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
        with self._cursor() as cur:
            cur.execute('''SELECT * FROM tasks WHERE id = %s''', (tid, ))
            return self._complete_task_row(next(cur), cur)

    def _complete_task_row(self, row, cur):
        decoded = []
        for x in row:
            if isinstance(x, buffer):
                x = str(x)
            if isinstance(x, basestring) and x.startswith('\\x'):
                x = x[2:].decode('hex')
            decoded.append(utils.decode_if_possible(x))
        task = dict(zip(self._task_fields, decoded))
        cur.execute('SELECT dependee FROM dependencies WHERE depender = %s', (task['id'], ))
        task['dependencies'] = [row[0] for row in cur]
        return task

    def update(self, tid, data):
        fields = []
        params = []
        for name in self._task_fields:
            try:
                value = data.pop(name)
            except KeyError:
                pass
            else:
                fields.append(name)
                value = utils.encode_if_required(value)
                if isinstance(value, basestring) and not value.isalnum():
                    value = buffer(value)
                params.append(value)

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
            cur.execute('''NOTIFY task_status, %s''', ('%d %s' % (tid, status), ))

    def mark_as_complete(self, tid, result):
        with self._cursor() as cur:
            cur.execute(
                '''UPDATE tasks SET status = 'complete', result = %s WHERE id = %s''',
                (utils.encode_if_required(result), tid),
            )
            cur.execute('''NOTIFY task_status, %s''', ['%d complete' % tid])
        future = self.futures.get(tid)
        if future:
            future.set_result(result)

    def mark_as_error(self, tid, exception):
        with self._cursor() as cur:
            cur.execute(
                '''UPDATE tasks SET status = 'error', result = %s WHERE id = %s''',
                (utils.encode_exception(exception), tid),
            )
            cur.execute('''NOTIFY task_status, %s''', ['%d error' % tid])
        future = self.futures.get(tid)
        future.set_exception(exception)

    def iter_pending_tasks(self):
        with self._cursor() as cur:
            cur.execute('''SELECT * FROM tasks WHERE status = 'pending' ''')
            rows = list(cur)
            for row in rows:
                yield self._complete_task_row(row, cur)



