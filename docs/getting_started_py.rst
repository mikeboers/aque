.. _getting_started_py:


Getting Started with Python
---------------------------

AQue has a concurrent.futures_ -ish API::

    >>> import aque
    >>> queue = aque.Queue()
    >>> 
    >>> future = queue.submit(my_awesome_function, 'arg1', 'arg2')
    >>> future.result()
    'my_awesome_result'

You can specify properties of the :ref:`task prototype <tasks>` with :meth:`~aque.queue.Queue.submit_ex`::

    >>> future = queue.submit_ex(my_awesome_function, cpus=4, memory='1GB')
    >>> future.result()
    'my_awesome_result that uses 4 CPUs'

AQue also has a Celery-ish API::

    >>> @queue.task(cpus=4)
    ... def async_function():
    ...     # do something great
    ...
    >>> future = async_function()
    >>> future.result()
    'great results'


.. _concurrent.futures: https://docs.python.org/3/library/concurrent.futures.html
