AQue: (Another async work Queue)
================================

AQue is a Python package to allow one to define a :abbr:`DAG (Directed Acyclic Graph)` of work that may be executed directly, or asynchronously on a set of servers.

A :class:`.Task` is a unit-of-work which can be independantly scheduled, run, and re-run. Tasks may have children or dependencies which must finish first and whose results can be used later.


Contents:

.. toctree::
   :maxdepth: 2

   tasks
   patterns



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

