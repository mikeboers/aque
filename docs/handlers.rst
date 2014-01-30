Task Handlers
============

A task handler is a callable which is responsible for interpreting a task definition and starting its execution. It is often enough to provide your own callable to an existing handler instead of making your own handlers.

The handler accepts a :class:`.Task` as its only argument, and calls :meth:`.Task.complete` when the task completes successfully or :meth:`.Task.error` when an error occours.

The ``generic`` Handler
-----------------------

The default ``generic`` handler calls ``func`` with the given ``args`` and ``kwargs``, and is easily defined::

    def generic(task):
        func = task['func']
        args = task.get('args', ())
        kwargs = task.get('kwargs', {})
        task.complete(func(*args, **kargs))
