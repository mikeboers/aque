AQue: (Another async work Queue)
================================

AQue is a Python package to allow one to define a :abbr:`DAG (Directed Acyclic Graph)` of work that may be executed directly, or asynchronously on a set of servers.

A :ref:`task <tasks>` is a unit-of-work which can be independantly scheduled, run, and re-run. Tasks may have dependencies which must finish first and whose results can be used later.


Getting Started with the Command Line
-------------------------------------

AQue has a command line API to allow you to run your existing scripts on your farm. The simplest command is ``aque submit``::

    $ aque submit echo hello
    123

This has submitted the task with ID 123. You can retrieve the results with ``aque status``::

    $ aque status -f id,status,result 123
    id,status,result
    123,success,0

The result of a command-line task is it's return code. In the near future we will also capture stdio, but you can do that manually::

    $ aque submit -- bash -c 'echo hello from $AQUE_TID > result.txt'
    124
    $ # wait for it to finish...
    $ cat result.txt
    hello from 124

``aque xargs`` is an xargs-like interface::

    $ seq 1 20 | aque xargs -n1 -- bash -c 'echo $AQUE_TID says $1 >> result.txt' procname
    145
    $ tail -f result.txt
    125 says 1
    126 says 2
    127 says 3
    ...


Contents
--------

.. toctree::
   :maxdepth: 2

   installation
   tasks
   queues
   patterns
   brokers
   exceptions
   locals



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

