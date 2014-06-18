import contextlib
import fnmatch
import grp
import itertools
import logging
import multiprocessing
import os
import pickle
import pprint
import pwd
import select
import socket
import subprocess
import sys
import threading
import time
import traceback
import signal

import psutil

from aque.brokers import get_broker
from aque.eventloop import SelectableEvent, EventLoop, StopSelection
from aque.exceptions import DependencyFailedError, DependencyResolutionError, PatternMissingError
from aque.futures import Future
from aque.local import _local
from aque.utils import decode_callable, parse_bytes, debug, get_mount


log = logging.getLogger(__name__)


CPU_COUNT = psutil.cpu_count()
MEM_TOTAL = psutil.virtual_memory().total
IS_ROOT = not os.getuid()
LOGIN = pwd.getpwuid(os.getuid()).pw_name
HOSTNAME = socket.gethostname().lower()



class BaseJob(object):

    def __init__(self, broker, task):

        self.broker = broker
        self.task = task
        self.id = task['id']

    def close(self):
        pass

    def start(self):
        pass

    def execute(self):
        try:

            encoded_pattern = self.task.get('pattern', 'generic')
            try:
                pattern_func = decode_callable(encoded_pattern, 'aque_patterns')
            except ValueError:
                pattern_func = None
            if pattern_func is None:
                raise PatternMissingError('cannot decode pattern from %r' % encoded_pattern)

            _local.task = self.task
            _local.broker = self.broker

            res = pattern_func(self.task)

        except KeyboardInterrupt:
            raise
        
        except Exception as e:
            self.broker.set_status_and_notify(self.id, 'error', e)
            log.exception('error during execution')

        else:
            self.broker.set_status_and_notify(self.id, 'success', res)


class ThreadJob(BaseJob):

    def start(self):
        self.finished = SelectableEvent()
        self.thread = threading.Thread(target=self.execute)
        self.thread.start()

    def close(self):
        self.finished.close()

    def to_select(self):
        return [self.finished.fileno()], [], []

    def on_select(self, rfds, wfds, xfds):
        if not self.thread.is_alive() or self.finished.is_set():
            raise StopSelection()

    def execute(self):
        try:
            super(ThreadJob, self).execute()
        finally:
            self.finished.set()


class ProcJob(BaseJob):

    def start(self):

        self.broker.bind('signal_task.%d' % self.id, self.on_signaled)

        o_rfd, o_wfd = os.pipe()
        e_rfd, e_wfd = os.pipe()

        # Start the actuall subprocess.
        if self.task.get('interpreter'):

            cmd = []
            if 'KS_DEV_ARGS' in os.environ:
                cmd.extend(('dev', '--bootstrap'))
            cmd.extend((
                self.task['interpreter'],
                '-m', 'aque.workersandbox.thecorner',
                str(self.id), # so that `top` and `ps` show something more interesting
            ))

            encoded_package = pickle.dumps((self.broker, self.task))
            self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=o_wfd, stderr=e_wfd, close_fds=True)
            self.proc.stdin.write(encoded_package)
            self.proc.stdin.close()
            self.is_alive = lambda: not (self.proc.poll() or self.proc.returncode is not None)

        else:
            i_rfd, i_wfd = os.pipe()
            self.proc = multiprocessing.Process(target=self._target, args=(i_rfd, o_wfd, e_wfd))
            self.proc.start()
            self.is_alive = self.proc.is_alive

            os.close(i_rfd)
            os.close(i_wfd)

        os.close(o_wfd)
        os.close(e_wfd)

        self.fd_map = {
            o_rfd: 1,
            e_rfd: 2,
        }
        self.fd_offsets = {
            o_rfd: 0,
            e_rfd: 0,
        }

        log.log(5, 'proc %d for task %d started' % (self.proc.pid, self.id))

    def to_select(self):
        return self.fd_map.keys(), [], []

    def on_select(self, rfds, wfds, xfds):

        for rfd in rfds:
            to_fd = self.fd_map.get(rfd)
            if to_fd is not None:
                x = os.read(rfd, 65536)
                if x:
                    log.log(5, '%d piped %s' % (self.id, x.encode('string-escape')))
                    self.broker.log_output_and_notify(self.id, to_fd, self.fd_offsets[rfd], x)
                    self.fd_offsets[rfd] += len(x)
                else:
                    os.close(rfd)
                    del self.fd_map[rfd]

        has_fds = self.fd_map
        has_life = self.is_alive()

        if not has_fds and not has_life:
            log.log(5, 'proc %d for task %d joined' % (self.proc.pid, self.id))
            raise StopSelection()

        elif not (has_fds and has_life) and (has_fds or has_life):
            log.log(5, 'proc %d for task %d is about to die; only %s' % (self.proc.pid, self.id, 'has fds' if has_fds else 'has life'))
        
        else:
            log.log(5, 'proc %d is alive with rfds %s' % (self.proc.pid, rfds))

    def _target(self, i_rfd, o_wfd, e_wfd):

        self.broker.after_fork()
        self.bootstrap()
        
        os.dup2(i_rfd, 0)
        os.close(i_rfd)
        os.dup2(o_wfd, 1)
        os.close(o_wfd)
        os.dup2(e_wfd, 2)
        os.close(e_wfd)

        self.execute()

    def bootstrap(self):

        # So that signals from `aque kill` will be passed down to any
        # child processes (many of which come from the "shell" pattern).
        os.setsid()

        if IS_ROOT:

            # Drop permissions.
            uid = pwd.getpwnam(self.task['user']).pw_uid
            try:
                gid = grp.getgrnam(self.task['group']).gr_gid
            except KeyError:
                gid = grp.getgrgid(uid).gr_gid

            os.setregid(gid, gid)
            os.setreuid(uid, uid)

        os.chdir(self.task['cwd'])

    def on_signaled(self, tids, signal):
        if self.proc.pid and self.is_alive():
            os.killpg(os.getpgid(self.proc.pid), signal)
            log.info('task %d was sent signal %d' % (self.id, signal))

    def close(self):
        self.broker.unbind('signal_task.%d' % self.id, self.on_signaled)




def procjob_execute():
    """Called within the subprocess to actually do the work."""
    encoded = sys.stdin.read()
    broker, task = pickle.loads(encoded)
    job = ProcJob(broker, task)
    job.bootstrap()
    job.execute()
    job.cleanup()



def task_cpus(task):
    cpus = task.get('cpus')
    if cpus is None:
        return 1.0
    cpus = float(cpus)

    # HACK: negative cpus are fractions of the total.
    if cpus < 0:
        cpus = CPU_COUNT / -cpus

    return cpus

def task_memory(task):
    memory = task.get('memory')
    return memory or 0


class Worker(object):

    def __init__(self, broker=None, max_cpus=None):
        self.broker = get_broker(broker)
        self._event_loop = self.broker._event_loop
        self._stopper = threading.Event()
        self.broker.bind('task_status.pending', lambda *args, **kwargs: None)

    def stop(self):
        self._stopper.set()

    def __del__(self):
        self.stop()

    def run_one(self):
        self._run(count=1, wait_for_more=False)

    def run_to_end(self):
        self._run(count=None, wait_for_more=False)

    def run_forever(self):
        self._run(count=None, wait_for_more=True)

    def _resources_left(self):
        cpus = CPU_COUNT
        memory = MEM_TOTAL
        for obj in self._event_loop.active:
            if isinstance(obj, BaseJob):
                cpus -= task_cpus(obj.task)
                memory -= task_memory(obj.task)
        return cpus, memory

    def _can_currently_satisfy_requirements(self, task, cpus, memory):

        # Obvious ones.
        if task_cpus(task) > cpus + 0.1:
            return False
        if task_memory(task) > memory:
            return False

        return True

    def _can_ever_satisfy_requirements(self, task):

        # Check the host globs.
        host_patterns = task.get('host')
        if host_patterns:
            if isinstance(host_patterns, basestring):
                host_patterns = [h.strip() for h in host_patterns.split(',')]
            for host in host_patterns:
                negate = host.startswith('!')
                host = host[1:] if negate else host
                passes = fnmatch.fnmatch(HOSTNAME, host.lower())
                if passes:
                    if negate:
                        log.debug('rejecting %d due to mismatched host' % task['id'])
                        return False
                    else:
                        break
            else:
                return False

        # If the worker is not root or unable to setuid, then we can only do
        # jobs for our own user.
        if (not IS_ROOT or not self.broker.can_fork) and LOGIN != task['user']:
            log.debug('rejecting %d due to mismatched user' % task['id'])
            return False

        if not self.broker.can_fork and os.getcwd() != task['cwd']:
            log.debug('rejecting %d due to mismatched cwd' % task['id'])
            return False

        # Make sure we have this user.
        try:
            pwd.getpwnam(task['user'])
        except KeyError:
            log.debug('rejecting %d due to unknown user' % task['id'])
            return False

        if task.get('platform') and task['platform'].lower() != sys.platform.lower():
            log.debug('rejecting %d due to mismatched platform' % task['id'])
            return False

        return True

    def _spawn_jobs(self, count):

        cpus, memory = self._resources_left()

        # Check that we haven't spawned too many yet AND there are enough
        # resources to spare.
        # TODO: wait until a reasonable amount of time has passed or we have
        #       notification that there are new jobs.
        task_iter = None
        while (count is None or count > 0) and cpus > 0 and memory > 0:

            task_iter = task_iter or self.iter_open_tasks()
            try:
                task = next(task_iter)
            except StopIteration:
                break

            # Shortcut for grouping tasks.
            if task.get('pattern', 'xxx') is None:
                self.broker.set_status_and_notify(task['id'], 'success', None)
                continue

            # Don't consider anything we are already working on.
            if any(task['id'] == job.id for job in self._event_loop.active if isinstance(job, BaseJob)):
                continue

            # TODO: track these so that we don't bother looking at the same
            # tasks over and over.
            if not self._can_ever_satisfy_requirements(task):
                continue
            if not self._can_currently_satisfy_requirements(task, cpus, memory):
                continue

            if not self.broker.acquire(task['id']):
                continue

            job = (ProcJob if self.broker.can_fork else ThreadJob)(self.broker, task)
            job.start()
            self._event_loop.add(job)

            cpus, memory = self._resources_left()

            count = count - 1 if count is not None else None

        return count

    def _run(self, count, wait_for_more):
        try:

            self._stopper.clear()
            self._event_loop.stop_thread()

            # The main loop.
            while not self._stopper.is_set():
                count = self._run_inner(count, wait_for_more)

            # Finish what we were doing.
            while any(isinstance(x, BaseJob) for x in self._event_loop.active):
                self._run_inner(-1, False)

        except StopIteration:
            pass
        finally:
            log.debug('worker is stopping')
            self._event_loop.resume_thread()

    def _run_inner(self, count, wait_for_more):

            count = self._spawn_jobs(count)

            # TODO: longer timeout once we listen to pending task events
            active_jobs = [x for x in self._event_loop.active if isinstance(x, BaseJob)]
            log.info("%d active jobs: %s" % (len(active_jobs), ', '.join(str(job.id) for job in active_jobs)))
            if active_jobs:
                self._event_loop.process(timeout=15.0)

            # Deal with any jobs that just stopped.
            job_just_finished = False
            for obj in self._event_loop.stopped:
                if isinstance(obj, BaseJob):
                    obj.close()
                    self.broker.release(obj.id)
                    job_just_finished = True

            self._event_loop.stopped[:] = []

            if not job_just_finished and not any(isinstance(x, BaseJob) for x in self._event_loop.active):
                if wait_for_more:
                    # XXX: don't need this once the above timeout is higher
                    log.info('waiting for more work...')
                    self._event_loop.process(timeout=60.0)
                else:
                    raise StopIteration()

            return count

    def calculate_priority(self, task):

        # Relative scales for different filesystems, weighted by the size of
        # the files being manipulated.
        fs_scales = []
        fs_weight = 0
        for path in task.get('io_paths') or ():
            try:
                stat = os.stat(path)
            except OSError as e:
                continue
            mount = get_mount(path)
            log.log(5, 'mount for %r is %r' % (path, mount))
            fs_scales.append({
                'devfs': 0.1,
                'proc': 0.1,
                'nfs4': 0.75,
            }.get(mount and mount.type, 1.0) * stat.st_size)
            fs_weight += stat.st_size
        fs_scale = sum(fs_scales) / fs_weight if fs_weight else 1.0

        log.debug('filesystem priority scale for %d is %.3f (%r / %d)' % (task['id'], fs_scale, fs_scales, fs_weight))
        
        return (

            # Higher priority values come first.
            -task.get('priority', 1000),

            # Faster file IO comes first.
            fs_scale,

            # Longer tasks come first (for scheduling benifits).
            -(task.get('duration') or 0),

            # A reasonable-ish fallback.
            task['id'],

        )

    def iter_open_tasks(self):

        pending_tasks = list(self.broker.search({'status': 'pending'}, ['id', 'status', 'dependencies', 'io_paths', 'duration']))

        task_cache = dict((t['id'], t) for t in pending_tasks)
        task_priorities = {}

        considered = set()
        while True:

            # Make sure we are only looking at pending tasks that we have not
            # already considered. (The pending check is required for the
            # MemoryBroker, which sometimes modifies tasks.)
            pending_tasks = [
                task for task in pending_tasks
                if task['status'] == 'pending' and task['id'] not in considered
            ]
            if not pending_tasks:
                break

            # Create dynamic priorities for every task, and sort by them.
            for task in pending_tasks:
                if task['id'] not in task_priorities:
                    task_priorities[task['id']] = self.calculate_priority(task)

            pending_tasks.sort(key=lambda task: task_priorities[task['id']])
            task = pending_tasks.pop(0)

            dependency_ids = task.get('dependencies') or []

            # Cache the ones we haven't seen before.
            uncached_ids = [tid for tid in dependency_ids if tid not in task_cache]
            if uncached_ids:
                task_cache.update(self.broker.fetch(uncached_ids, fields=['id', 'status', 'dependencies']))

            skip_task = False

            for tid in dependency_ids:

                dep = task_cache.get(tid)

                if not dep:
                    log.warning('task %r is missing dependency %r' % (task['id'], tid))
                    self.broker.set_status_and_notify(task['id'], 'error',
                        DependencyResolutionError('task %r does not exist' % tid),
                    )
                    skip_task = True
                    continue

                if dep['status'] == 'pending':
                    skip_task = True
                    pending_tasks.append(dep)

                elif dep['status'] == 'paused':
                    skip_task = True

                elif dep['status'] != 'success':
                    skip_task = True
                    log.info('task %r has failed dependency %r' % (task['id'], tid))
                    self.broker.set_status_and_notify(task['id'], 'error',
                        DependencyFailedError('task %r has status %r' % (tid, dep['status']))
                    )

            if skip_task:
                continue

            yield self.broker.fetch(task['id'])

