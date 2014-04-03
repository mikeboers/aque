import logging
import os
import threading
import time
from select import select
from Queue import Empty


log = logging.getLogger(__name__)


class StopSelection(Exception):
    pass


class SelectableEvent(object):
    """Like ``threading.Event``, but ``select``able."""

    def __init__(self):
        self._rfd, self._wfd = os.pipe()

    def __del__(self):
        self.close()

    def close(self):
        if self._rfd is not None:
            os.close(self._rfd)
            os.close(self._wfd)
            self._rfd = None

    def wait(self, timeout=None):
        r, _, _ = select([self._rfd], [], [], timeout)
        return bool(r)

    def is_set(self):
        return self.wait(0)

    def clear(self):
        if self.is_set():
            os.read(self._rfd, 1)

    def set(self):
        if not self.is_set():
            os.write(self._wfd, 'x')

    def fileno(self):
        return self._rfd


class EventLoop(object):

    def __init__(self):

        self.active = []
        self.stopped = []

        self.timers = []
        self._zero_time = time.time()
        self._last_time = 0

        # This is used to signal of the thread has been suspended or not.
        self._interrupt = SelectableEvent()
        self._state_lock = threading.RLock()
        self._thread_should_stop = False
        self._thread_stopped = threading.Event()
        self._thread = None

    def add_timer(self, timeout, func):
        with self._state_lock:
            self.timers.append((timeout, func))
            self._interrupt.set()

    def remove_timer(self, func_to_remove):
        with self._state_lock:
            self.timers = [(timeout, func) for timeout, func in self.timers if func != func_to_remove]
            self._interrupt.set()

    def add(self, obj):
        with self._state_lock:
            self.active.append(obj)
            self._interrupt.set()

    def stop(self, obj):
        with self._state_lock:
            self.active.remove(obj)
            self.stopped.append(obj)
            self._interrupt.set()

    def remove(self, obj):
        with self._state_lock:
            try:
                self.active.remove(obj)
            except ValueError:
                pass
            try:
                self.stopped.remove(obj)
            except ValueError:
                pass
            self._interrupt.set()

    def process(self, timeout=None):

        self._interrupt.clear()
        to_select = [self._interrupt.fileno()], [], []

        objs_and_fds = []
        for obj in self.active[:]:
            try:
                fds = obj.to_select()
            except StopSelection:
                self.stop(obj)
            else:
                objs_and_fds.append((obj, fds))
                for all_, new in zip(to_select, fds):
                    all_.extend(new)

        # Bail if it is only the interrupt we are listening to.
        if sum(map(len, to_select)) == 1 and not self.timers:
            return

        # Establish when the next timer will tick, and adjust timeout accordingly.
        current_time = time.time() - self._zero_time
        timers = []
        for interval, func in self.timers:
            next_tick = self._last_time + interval - (self._last_time % interval)
            time_to_next_tick = next_tick - current_time
            timers.append((next_tick, func))
            timeout = time_to_next_tick if timeout is None else min(timeout, time_to_next_tick)
        timeout = None if timeout is None else max(0, timeout)

        log.log(5, '%d fds to select from %d objects and %d timers' % (sum(map(len, to_select)), len(self.active), len(self.timers)))

        selected = select(to_select[0], to_select[1], to_select[2], timeout)
        selected = [set(x) for x in selected]


        # Trigger some timers!
        # Notice that we will only trigger the timer once even if it would have
        # triggered several times since it was run last.
        self._last_time = current_time = time.time() - self._zero_time
        for next_tick, func in timers:
            # print current_time, next_tick, func
            if current_time >= next_tick:
                try:
                    func()
                except StopSelection:
                    self.remove_timer(func)

        for obj, fds in objs_and_fds:
            fds = [s.intersection(x) for x, s in zip(fds, selected)]
            try:
                obj.on_select(*fds)
            except StopSelection:
                self.stop(obj)

        return sum(len(x) for x in selected)

    def start_thread(self):
        """Make sure the thread is running, or run it."""
        with self._state_lock:
            if self._thread or self._thread_should_stop:
                return
            self._thread = threading.Thread(target=self._thread_target)
            self._thread.daemon = True
            self._thread.start()

    def stop_thread(self):
        """Stop, and wait for it to stop (iff running)."""
        with self._state_lock:
            was_stopped = self._thread is None
            self._thread_should_stop = True
            self._interrupt.set()
        if not was_stopped:
            self._thread_stopped.wait()

    def resume_thread(self):
        """Re-start the thread, but only if it was originally stopped."""
        with self._state_lock:
            if self._thread_should_stop:
                self._thread_should_stop = False
                self.start_thread()

    def _thread_target(self):
        self._thread_stopped.clear()
        try:
            while True:
                with self._state_lock:
                    if self._thread_should_stop:
                        return
                    self._interrupt.clear()
                self.process()
        finally:
            with self._state_lock:
                self._thread_stopped.set()
                self._thread = None


