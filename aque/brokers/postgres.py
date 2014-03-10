import contextlib
import json
import threading
import select
from Queue import Queue, Empty

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

        self._notify_stopper = utils.WaitableEvent()
        self._notify_lock = threading.Lock()
        self._notify_thread = None
        self._notify_queues = {}

        self.init()

    def __del__(self):
        self.close()

    def close(self):
        self._notify_stopper.set()

    def _notify_start(self):
        with self._notify_lock:
            if not self._notify_thread:
                self._notify_thread = threading.Thread(target=self._event_thread_target)
                self._notify_thread.daemon=True
                self._notify_thread.start()

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
            return self._complete_task_row(next(cur), cur)

    def _encode(self, x):
        x = utils.encode_if_required(x)
        if isinstance(x, basestring) and not x.isalnum():
            x = buffer(x)
        return x

    def _decode(self, x):
        if isinstance(x, buffer):
            x = str(x)
        if isinstance(x, basestring) and x.startswith('\\x'):
            x = x[2:].decode('hex')
        return utils.decode_if_possible(x)

    def _complete_task_row(self, row, cur):
        decoded = [self._decode(x) for x in row]
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
                params.append(self._encode(value))

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

    def iter_pending_tasks(self):
        with self._cursor() as cur:
            cur.execute('''SELECT * FROM tasks WHERE status = 'pending' ''')
            rows = list(cur)
            for row in rows:
                yield self._complete_task_row(row, cur)

    def wait_for(self, tid, timeout=None):

        raise NotImplementedError()

        # Schedule ourselves to receive events.
        queue = Queue()
        with self._notify_lock:
            self._notify_queues.setdefault(tid, []).append(queue)

        # Lets take a look at the actual status, however.
        with self._cursor() as cur:
            cur.execute('SELECT status FROM tasks WHERE id = %s', [tid])
            status = next(cur)[0]

        # Wait for a non-pending status.
        while status == 'pending':
            try:
                status = queue.get(True, timeout)
            except Empty:
                break

        # Unschedule ourselves.
        self._notify_queues[tid].remove(queue)

        return status

    def _event_thread_target(self):
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

