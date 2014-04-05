AQue: (Another async work Queue)
================================

AQue is a Python package to allow one to define a :abbr:`DAG (Directed Acyclic Graph)` of work that may be executed directly, or asynchronously on a set of servers.

A :ref:`task <tasks>` is a unit-of-work which can be independantly scheduled, run, and re-run. Tasks may have dependencies which must finish first and whose results can be used later.


Overview
--------

.. toctree::
    :maxdepth: 2

    installation
    getting_started_cli
    getting_started_py
    tasks
    patterns


Python API
----------

.. toctree::
    :maxdepth: 2

    queues
    brokers
    exceptions
    locals



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

