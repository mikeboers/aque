.. _patterns:

Patterns
========

A task pattern is a callable which is responsible for interpreting a task definition and managing its execution. It is often enough to provide your own callable to an existing pattern instead of making your own patterns.

The pattern accepts two positional arguments: a :class:`.Broker` to report results to, and a dict :ref:`task prototype <tasks>`. The pattern should call :meth:`.Broker.mark_as_success` when the task completes successfully or :meth:`.Broker.mark_as_error` when an error occurs.


The ``generic`` Pattern
-----------------------

The default ``generic`` pattern calls ``func`` with the given ``args`` and ``kwargs``, and is easily defined::

    def generic(broker, task):
        func = task['func']
        args = task.get('args', ())
        kwargs = task.get('kwargs', {})
        broker.mark_as_success(func(*args, **kargs))
