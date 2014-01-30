import functools


registry = {}


def handler(name, func=None):
    """Decorator to register a handler.

    :param str name: Optional name for the handler.
    
    ::
        @handler
        def my_handler(task):
            pass

        @handler('explicit_name')
        def another_handler(task):
            pass

    """

    if not isinstance(name, basestring):
        func = name
        name = func.__name__

    if not func:
        return functools.partial(handler, name)

    registry[name] = func
    return func
