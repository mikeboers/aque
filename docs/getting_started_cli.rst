.. _getting_started_cli:

Getting Started with the Command Line
=====================================


``aque submit``
---------------

AQue has a command line API to allow you to run your existing scripts on your farm. The simplest command is ``aque submit``::

    $ aque submit echo hello
    123


``aque status``
---------------

This has submitted the task with ID 123. You can retrieve the results with ``aque status``::

    $ aque status -f id,status,result 123
    id,status,result
    123,success,0


``aque output``
---------------

The result of a command-line task is it's return code. We also capture the stdio of tasks::

    $ aque submit --shell 'echo hello from $AQUE_TID'
    124
    $ aque output -w
    hello from 123

    $ # You can also wait from the submit directly:
    $ aque submit --wait --shell 'echo hello from $AQUE_TID'
    125


``aque xargs``
--------------

``aque xargs`` is an xargs-like interface::

    $ seq 1 20 | aque xargs -n1 bash -c 'echo $AQUE_TID says $1 >> result.txt' procname
    145
    $ tail -f result.txt
    125 says 1
    126 says 2
    127 says 3
    ...


``aque kill``
-------------

Individual tasks can be killed (or signaled, if you prefer)::

    $ tid=$(aque submit --shell 'echo before; sleep 10; echo after')
    $ echo $tid
    146
    $ aque kill $tid
    $ aque status --csv id,status,result $tid
    146,killed,None
    $ aque output
    before
