import os
import time
from select import select
from Queue import Empty


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

    def add_timer(self, timeout, func):
        self.timers.append((timeout, func))

    def remove_timer(self, func_to_remove):
        self.timers = [(timeout, func) for timeout, func in self.timers if func != func_to_remove]

    def add(self, obj):
        self.active.append(obj)

    def stop(self, obj):
        self.active.remove(obj)
        self.stopped.append(obj)

    def remove(self, obj):
        try:
            self.active.remove(obj)
        except ValueError:
            pass
        try:
            self.stopped.remove(obj)
        except ValueError:
            pass

    def process(self, timeout=None):

        to_select = [], [], []

        objs_and_fds = []
        for obj in self.active[:]:
            try:
                fds = obj.to_select()
            except StopSelection:
                done.append(obj)
                self.stop(obj)
            else:
                objs_and_fds.append((obj, fds))
                for all_, new in zip(to_select, fds):
                    all_.extend(new)

        if not any(to_select) and not self.timers:
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

        selected = select(to_select[0], to_select[1], to_select[2], timeout)
        selected = [set(x) for x in selected]

        # Trigger some timers!
        self._last_time = current_time = time.time() - self._zero_time
        for next_tick, func in timers:
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
