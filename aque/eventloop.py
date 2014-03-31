import os
from select import select
from Queue import Empty


class StopSelection(Exception):
    pass


class Event(object):
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


class SelectableQueue(object):

    def __init__(self):
        self._rfd, self._wfd = os.pipe()
        self._objs = []

    def __del__(self):
        os.close(self._rfd)
        os.close(self._wfd)

    def put(self, obj):
        self._objs.append(obj)
        os.write(self._wfd, 'x')

    def get(self, block=True):
        if block:
            os.read(self._rfd, 1)
        try:
            obj = self._objs.pop(0)
        except IndexError:
            raise Empty()
        if not block:
            os.read(self._rfd, 1)

    def to_select(self):
        return [self._rfd], (), ()

    def on_select(self, *args):
        pass


class EventLoop(object):

    def __init__(self):
        self.selectables = []

    def add(self, obj):
        self.selectables.append(obj)

    def remove(self, obj):
        self.selectables.remove(obj)

    def process(self, timeout=None):

        to_select = [], [], []
        done = []

        objs_and_fds = []
        for obj in self.selectables[:]:
            try:
                fds = obj.to_select()
            except StopSelection:
                done.append(obj)
                self.remove(obj)
            else:
                objs_and_fds.append((obj, fds))
                for all_, new in zip(to_select, fds):
                    all_.extend(new)

        if not any(to_select):
            return done

        selected = select(to_select[0], to_select[1], to_select[2], timeout)
        selected = [set(x) for x in selected]

        for obj, fds in objs_and_fds:
            fds = [s.intersection(x) for x, s in zip(fds, selected)]
            try:
                obj.on_select(*fds)
            except StopSelection:
                done.append(obj)
                self.remove(obj)

        return done

    def loop(self, timeout=None):
        while self.selectables:
            self.process(timeout)



