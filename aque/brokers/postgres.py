import contextlib
import json
import threading
import select
import logging
from Queue import Queue, Empty

import psycopg2.pool
import psycopg2.extras
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
            # kwargs['connection_factory'] = pg.extras.LoggingConnection
            self._pool = pg.pool.ThreadedConnectionPool(0, 4, **kwargs)

        self._inspect_schema()

        self._notify_stopper = utils.WaitableEvent()
        self._notify_lock = threading.Lock()
        self._notify_thread = None

    def __del__(self):
        self.close()

    def close(self):
        self._notify_stopper.set()

    def _notify_start(self):
        with self._notify_lock:
            if not self._notify_thread:
                self._notify_thread = threading.Thread(target=self._notify_target)
                self._notify_thread.daemon=True
                self._notify_thread.start()

    @contextlib.contextmanager
    def _connect(self):
        conn = self._pool.getconn()
        # conn.initialize(logging.getLogger('aque.sql'))
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

    def update_schema(self):
        with self._cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                dependencies INTEGER[],
                status TEXT NOT NULL DEFAULT 'creating',
                priority INTEGER NOT NULL DEFAULT 1000,
                "user" TEXT,
                "group" TEXT,
                name TEXT,
                pattern BYTEA,
                func BYTEA,
                args BYTEA,
                kwargs BYTEA,
                result BYTEA
            )''')

    def _inspect_schema(self):
        with self._cursor() as cur:
            # Determine what rows we actually have.
            cur.execute('''SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'tasks' ''')
            self._task_fields = tuple(cur)

    def destroy_schema(self):
        with self._cursor() as cur:
            cur.execute('''DROP TABLE IF EXISTS dependencies''')
            cur.execute('''DROP TABLE IF EXISTS tasks''')

    def get_future(self, tid):
        future = super(PostgresBroker, self).get_future(tid)
        self._notify_start()
        return future

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
            return self._decode_task(next(cur))

    def _encode(self, x):
        x = utils.encode_if_required(x)
        if isinstance(x, basestring) and not x.isalnum():
            x = pg.Binary(x)
        return x

    def _decode(self, x):
        if isinstance(x, buffer):
            x = str(x)
        if isinstance(x, basestring) and x.startswith('\\x'):
            x = x[2:].decode('hex')
        return utils.decode_if_possible(x)

    def _decode_task(self, row):
        return dict((name, self._decode(x)) for (name, type_), x in zip(self._task_fields, row))

    def update(self, tid, data):

        fields = []
        params = []
        for name, type_ in self._task_fields:
            try:
                value = data.pop(name)
            except KeyError:
                pass
            else:
                fields.append(name)
                if type_ == 'bytea':
                    value = self._encode(value)
                params.append(value)

        if data:
            raise ValueError('unexpected keys: %s' % ', '.join(sorted(data)))

        query = 'UPDATE tasks SET %s WHERE id = %%s' % ', '.join('"%s" = %%s' % name for name in fields)
        params.append(tid)

        with self._cursor() as cur:
            cur.execute(query, params)

    def delete(self, tid):
        with self._cursor() as cur:
            cur.execute('DELETE FROM tasks WHERE id = %s', [tid])
    
    def set_status_and_notify(self, tid, status):
        with self._cursor() as cur:
            cur.execute('''UPDATE tasks SET status = %s WHERE id = %s''', (status, tid))
            cur.execute('''NOTIFY task_status, %s''', [json.dumps({
                'id': tid,
                'status': status,
            })])

    def mark_as_success(self, tid, result):
        with self._cursor() as cur:
            cur.execute(
                '''UPDATE tasks SET status = 'success', result = %s WHERE id = %s''',
                [self._encode(result), tid],
            )
            cur.execute('''NOTIFY task_status, %s''', [json.dumps({
                'id': tid,
                'status': 'success',
            })])

    def mark_as_error(self, tid, exception):
        with self._cursor() as cur:
            cur.execute(
                '''UPDATE tasks SET status = 'error', result = %s WHERE id = %s''',
                [self._encode(exception), tid],
            )
            cur.execute('''NOTIFY task_status, %s''', [json.dumps({
                'id': tid,
                'status': 'error',
            })])

    def iter_tasks(self, **kwargs):
        with self._cursor() as cur:

            if kwargs:
                items = sorted(kwargs.iteritems())
                clause = 'WHERE ' + ' AND '.join('%s = %%s' % k for k, v in items)
                params = [v for k, v in items]
            else:
                clause = ''
                params = []

            cur.execute('''SELECT * FROM tasks %s''' % clause, params)
            rows = list(cur)

        for row in rows:
            try:
                yield self._decode_task(row)
            except (ImportError, ValueError):
                pass

    def _notify_target(self):
        with self._connect() as conn:

            cur = conn.cursor()
            cur.execute('LISTEN task_status')
            conn.commit()

            while not self._notify_stopper.is_set():
                r, _, _ = select.select([self._notify_stopper, conn], [], [], 1.0)
                if conn in r:
                    conn.poll()
                    while conn.notifies:
                        self._dispatch_notification(conn.notifies.pop())

    def _dispatch_notification(self, message):
        payload = json.loads(message.payload)
        future = self.futures.get(payload['id'])
        if not future:
            return
        if payload['status'] == 'success':
            with self._cursor() as cur:
                cur.execute('SELECT result FROM tasks WHERE id = %s', [payload['id']])
                future.set_result(self._decode(next(cur)[0]))
        elif payload['status'] == 'error':
            with self._cursor() as cur:
                cur.execute('SELECT result FROM tasks WHERE id = %s', [payload['id']])
                future.set_exception(self._decode(next(cur)[0]))

