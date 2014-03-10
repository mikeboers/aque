.. _patterns:

Patterns
========

A task pattern is a callable which is responsible for interpreting a task definition and managing its execution. It is often enough to provide your own callable to an existing pattern instead of making your own patterns.

The pattern accepts three positional arguments: a :class:`.Broker` to pull task data from and report results to, a dict :ref:`task prototype <tasks>`, and a scalar task ID. The pattern calls :meth:`.Broker.mark_as_success` when the task completes successfully or :meth:`.Broker.mark_as_error` when an error occours.


The ``generic`` Pattern
-----------------------

The default ``generic`` pattern calls ``func`` with the given ``args`` and ``kwargs``, and is easily defined::

    def generic(broker, proto, tid):
        func = proto['func']
        args = proto.get('args', ())
        kwargs = proto.get('kwargs', {})
        broker.mark_as_success(func(*args, **kargs))
