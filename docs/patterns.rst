.. _patterns:

Patterns
========

A task pattern is a callable which is responsible for interpreting a task definition and managing its execution. It is often enough to provide your own callable to an existing pattern instead of making your own patterns.

The pattern is passed a dict :ref:`task prototype <tasks>`. The pattern should simply return the result, and any exceptions raised by the pattern will be reported as errors.

If you require access to the :class:`.Broker`, then use :func:`~aque.local.current_broker`.


The ``generic`` Pattern
-----------------------

The default ``generic`` pattern calls ``func`` with the given ``args`` and ``kwargs``, and is easily defined::

    def generic(task):
        func = task['func']
        args = task['args']
        kwargs = task['kwargs']
        return func(*args, **kargs)
