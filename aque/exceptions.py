
class DependencyResolutionError(RuntimeError):
    """Raised when task dependencies cannot be resolved.

    This may be raised by :meth:`.Queue.submit` (if it is possible to detect at
    that time), or by :meth:`.Task.result` later.
    
    """

class PatternMissingError(RuntimeError):
    """Raised by :meth:`.Task.result` when the :ref:`pattern <patterns>` can't
    be identified."""

class DependencyFailedError(RuntimeError):
    """Raises by :meth:`.Task.result` when a dependency of the task failed."""
