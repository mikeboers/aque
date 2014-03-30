import threading


_local = threading.local()


def current_task():
    """Get the :ref:`task prototype <tasks>` that is being run, or None."""
    try:
        return _local.task
    except AttributeError:
        pass


def current_broker():
    """Get the :class:`.Broker` for the task being run, or None."""
    try:
        return _local.broker
    except AttributeError:
        pass
