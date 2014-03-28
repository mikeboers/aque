
class DependencyError(RuntimeError):
    """Raised when task dependencies cannot be resolved.

    This may be raised by :meth:`.Queue.submit`
    (if it is possible to detect at that time), or at task scheduling time.
    
    """

class TaskIncomplete(RuntimeError):
    """Raised by :meth:`.Task.result` when the :ref:`pattern <patterns>` running
    the task did not complete."""

class TaskError(RuntimeError):
    """Raised by :meth:`.Task.result` when the task errored without raiding an exception."""

class PreconditionFailed(RuntimeError):
    """Raises by :meth:`.Task.result` when a dependency of the task failed."""
