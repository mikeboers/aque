Job Handlers
============

A job handler is a callable which is responsible for interpreting a job definition and starting its execution. It is often enough to provide your own callable to an existing handler instead of making your own handlers.

The handler accepts a :class:`.Job` as its only argument, and calls :meth:`.Job.complete` when the job completes successfully or :meth:`.Job.error` when an error occours.

The ``generic`` Handler
-----------------------

The default ``generic`` handler calls ``func`` with the given ``args`` and ``kwargs``, and is easily defined::

    def generic(job):
        func = job['func']
        args = job.get('args', ())
        kwargs = job.get('kwargs', {})
        job.complete(func(*args, **kargs))
