from Queue import Queue, Empty
import contextlib
import datetime
import functools
import json
import logging
import os
import pickle
import select
import threading
import time

import psutil
import psycopg2.pool
import psycopg2.extras
import psycopg2 as pg

import aque.utils as utils
from aque.brokers.base import Broker


log = logging.getLogger(__name__)


schema_migrations = []
def patch(func=None, name=None):
    if func is None:
        return functools.partial(patch, name=name)

    name = name or func.__name__
    name = name.strip('_')
    schema_migrations.append((name, func))

    return func


@patch
def create_task_table(cur):
    cur.execute('''CREATE TABLE tasks (

        id SERIAL PRIMARY KEY,
        name TEXT,

        dependencies INTEGER[],

        -- requirements
        cpus REAL, -- fractional CPUs are okay
        memory INTEGER, -- in bytes
        platform TEXT,
        host TEXT,

        status TEXT NOT NULL DEFAULT 'creating',
        creation_time TIMESTAMP NOT NULL DEFAULT localtimestamp,
        first_active TIMESTAMP,
        last_active TIMESTAMP,

        -- priority
        priority INTEGER NOT NULL DEFAULT 1000,
        io_paths TEXT[],
        duration INTEGER,

        -- execution
        pattern BYTEA,
        func BYTEA,
        args BYTEA,
        kwargs BYTEA,

        -- environmental
        "user" TEXT,
        "group" TEXT,
        cwd TEXT,
        environ BYTEA,

        -- the final result, or exception (depending on status)
        result BYTEA,

        -- everything else
        extra BYTEA

    )''')
    cur.execute('CREATE INDEX tasks_status_index ON tasks (id, status)')


@patch
def create_output_log_table(cur):
    cur.execute('''CREATE TABLE output_logs (
        task_id SERIAL NOT NULL,
        ctime TIMESTAMP NOT NULL DEFAULT localtimestamp,
        fd SMALLINT NOT NULL,
        content TEXT NOT NULL
    )''')
    cur.execute('CREATE INDEX output_logs_index ON output_logs (task_id)')


class PostgresBroker(Broker):
    """A :class:`.Broker` which uses Postgresql_ as a data store and event dispatcher.

    .. _Postgresql: http://www.postgresql.org/
    """

    @classmethod
    def from_url(cls, parts):
        return cls(host=parts.netloc, database=parts.path.strip('/').lower())

    def __init__(self, **kwargs):

        self._notify_conn = None
        self._notify_lock = threading.Lock()
        self._notify_thread = None

        self._kwargs = kwargs
        self._pool = self._open_pool()
        self._reflect()

        super(PostgresBroker, self).__init__()


        self._heartbeat_lock = threading.Lock()
        self._heartbeat_thread = None
        self._captured_tids = []

    def _open_pool(self):
        return pg.pool.ThreadedConnectionPool(0, 4 * psutil.cpu_count(), **self._kwargs)

    def after_fork(self):
        self._pool = self._open_pool()

    def __del__(self):
        self.close()

    def close(self):
        pass
    
    def _notify_start(self):
        with self._notify_lock:
            if not self._notify_thread:
                self._notify_thread = threading.Thread(target=self._notify_target)
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

    def update_schema(self):

        with self._cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS schema_migrations (
                id SERIAL PRIMARY KEY,
                ctime TIMESTAMP NOT NULL DEFAULT localtimestamp,
                name TEXT NOT NULL
            )''')
            cur.execute('SELECT name from schema_migrations')
            applied_patches = set(row[0] for row in cur)

        for name, func in schema_migrations:
            if name not in applied_patches:
                log.info('applying schema migration %s' % name)
                with self._cursor() as cur:
                    func(cur)
                    cur.execute('INSERT INTO schema_migrations (name) VALUES (%s)', [name])

    def _reflect(self):
        with self._cursor() as cur:
            cur.execute('''SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'tasks' ORDER BY ordinal_position''')
            self._task_field_types = dict(cur)

    def destroy_schema(self):
        with self._cursor() as cur:
            cur.execute('''DROP TABLE IF EXISTS schema_migrations''')
            cur.execute('''DROP TABLE IF EXISTS tasks''')
            cur.execute('''DROP TABLE IF EXISTS output_logs''')

    def get_future(self, tid):
        future = super(PostgresBroker, self).get_future(tid)
        self._notify_start()
        return future

    def _create_many(self, prototypes):
        fields, encoded = self._encode_many(prototypes)
        with self._cursor() as cur:
            params = [[e[f] for f in fields] for e in encoded]
            fields = ', '.join('"%s"' % f for f in fields)
            params_pattern = ','.join(["%s"] * len(params[0]))
            params = [cur.mogrify(params_pattern, p) for p in params]
            params = ', '.join('(%s)' % m for m in params)
            try:
                cur.execute('''INSERT INTO tasks (%s) VALUES %s RETURNING id''' % (fields, params))
            except:
                log.exception(repr(prototypes))
                raise
            tids = [row[0] for row in cur]
        return [self.get_future(tid) for tid in tids]

    def _fetch_many(self, tids, fields):
        if not tids:
            return {}
        fields = ', '.join('"%s"' % f for f in fields) if fields else '*'
        with self._cursor() as cur:
            cur.execute('''SELECT %s FROM tasks WHERE id = ANY(%%s)''' % fields, [list(tids)])
            rows = list(cur)
        tasks = {}
        for row in rows:
            task = self._decode_task(cur, row)
            tasks[task['id']] = task
        return tasks

    def _encode(self, field, x):
        if self._task_field_types.get(field) == 'bytea':
            return pg.Binary(pickle.dumps(x, protocol=-1))
        else:
            return x

    def _decode(self, field, x):
        if isinstance(x, buffer):
            return pickle.loads(x)
        else:
            return x

    def _encode_task(self, task):
        extra = {}
        res = {}
        for k, v in task.iteritems():
            if k in self._task_field_types:
                res[k] = self._encode(k, v)
            else:
                extra[k] = v

        if extra:
            res['extra'] = self._encode('extra', extra)

        res.pop('id', None)

        return res

    def _encode_many(self, tasks):
        keys = set(tasks[0])
        fields = sorted(keys)
        encoded = []
        for t in tasks:
            if set(t) != keys:
                raise ValueError('prototypes do not share keys')
            e = self._encode_task(t)
            encoded.append(e)
        return fields, encoded

    def _decode_task(self, cur, row):
        task = dict((field[0], self._decode(field[0], value)) for field, value in zip(cur.description, row))
        task.update(task.pop('extra', None) or {})
        return task

    def _delete_many(self, tids):
        tids = list(tids)
        with self._cursor() as cur:
            cur.execute('DELETE FROM tasks WHERE id = ANY(%s)', [tids])
            cur.execute('DELETE FROM output_logs WHERE task_id = ANY(%s)', [tids])
    
    def bind(self, event, callback=None):
        x = super(PostgresBroker, self).bind(event, callback)
        if x:
            return x
        if self._notify_conn is not None:
            cur = self._notify_conn.cursor()
            cur.execute('LISTEN "%s"' % event)
            self._notify_conn.commit()

        self._notify_start()

    def unbind(self, event, callback):
        super(PostgresBroker, self).bind(event, callback)
        if self._notify_conn and not self._bound_callbacks[event]:
            cur = self._notify_conn.cursor()
            cur.execute('UNLISTEN "%s"' % event)
            self._notify_conn.commit()

    def _send_remote_events(self, events, args, kwargs):
        payload = json.dumps([args, kwargs])
        with self._cursor() as cur:
            for e in events:
                cur.execute('NOTIFY "%s", %%s' % e, [payload])

    def _set_status(self, tids, status, result):
        tids = [tids] if isinstance(tids, int) else list(tids)
        encoded = self._encode('result', result)
        log.debug('setting status of %r to %r' % (tids, status))
        with self._cursor() as cur:
            cur.execute('''UPDATE tasks SET status = %s, result = %s WHERE id = ANY(%s)''', [status, encoded, tids])

    def _log_output(self, tid, fd, content):
        with self._cursor() as cur:
            cur.execute('INSERT INTO output_logs (task_id, fd, content) VALUES (%s, %s, %s)', [
                tid,
                fd,
                content.encode('string-escape'),
            ])

    def get_output(self, tids):
        with self._cursor() as cur:
            cur.execute('SELECT task_id, ctime, fd, content FROM output_logs WHERE task_id = ANY(%s) ORDER BY ctime', [tids])
            return [(task_id, ctime, fd, content.decode('string-escape')) for task_id, ctime, fd, content in cur]

    def search(self, filter=None, fields=None):

        fields = ', '.join('"%s"' % f for f in fields) if fields else '*'

        if filter:
            items = sorted(filter.iteritems())
            clause = 'WHERE ' + ' AND '.join('"%s" = %%s' % k for k, v in items)
            params = [v for k, v in items]
        else:
            clause = ''
            params = []

        query = '''SELECT %s FROM tasks %s''' % (fields, clause)

        with self._cursor() as cur:

            cur.execute(query, params)
            rows = list(cur)

        for row in rows:
            yield self._decode_task(cur, row)

    def _notify_target(self):

        self._notify_conn = conn = self._pool.getconn()
        cur = conn.cursor()
        for event in self._bound_callbacks:
            cur.execute('LISTEN "%s"' % event)
        conn.commit()

        while True:
            r, _, _ = select.select([conn], [], [], 60)
            if not r:
                continue
            conn.poll()
            while conn.notifies:
                message = conn.notifies.pop()
                if message.pid == os.getpid():
                    continue
                args, kwargs = json.loads(message.payload)
                self._dispatch_local_events([message.channel], args, kwargs)

    def acquire(self, tid):

        with self._cursor() as cur:

            cur.execute('SELECT status, last_active FROM tasks WHERE id = %s FOR UPDATE', [tid])
            status, last_active = next(cur, (None, None))

            if status != 'pending':
                log.debug('capturing task %d failed; currently %s' % (tid, status))
                return False

            cur.execute('SELECT localtimestamp')
            current_time = next(cur)[0]

            age = (current_time - last_active).total_seconds() if last_active else None
            if age is not None and age <= 30:
                log.debug('capturing task %d failed; currently %ss old' % (tid, age))
                return

            cur.execute('UPDATE tasks SET first_active = %s, last_active = %s WHERE id = %s', [current_time, current_time, tid])

        # Spin up the heartbeat thread.
        # TODO: add this to the event loop, somehow.
        with self._heartbeat_lock:
            self._captured_tids.append(tid)
            if not self._heartbeat_thread:
                self._heartbeat_thread = threading.Thread(target=self._heartbeat)
                self._heartbeat_thread.daemon = True
                self._heartbeat_thread.start()

        return True

    def release(self, tid):
        with self._cursor() as cur:
            cur.execute('UPDATE tasks SET last_active = NULL WHERE id = %s', [tid])
        with self._heartbeat_lock:
            self._captured_tids.remove(tid)

    def _heartbeat(self):
        while True:
            with self._heartbeat_lock:
                if self._captured_tids:
                    with self._cursor() as cur:
                        cur.execute('UPDATE tasks SET last_active = localtimestamp WHERE id = ANY(%s)', [list(self._captured_tids)])
            time.sleep(15)


