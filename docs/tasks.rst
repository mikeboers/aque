.. _tasks:

Tasks
====

As the lowest level, task prototypes are represented as dictionaries with a few special keys:

func
    The callable which does the work of this task.
args
    The positional arguments to call the "func" with.
kwargs
    The keyword arguments to call the "func" with.
dependencies
    Other tasks that must complete successfully before this task will run; either other prototypes, or task instance IDs.
children
    Like dependencies, but inherit some config and scheduling from their parent.
user
    Which user to run the task as (for async workers).
group
    Which group to run the task as (for async workers).
priority
    Value to schedule this task relative to others; larger values are more important.
pattern
    General calling pattern of this task. Defaults to ``"generic"`` which calls
    ``func(*args, **kwargs)``. See :ref:`patterns <patterns>`.
