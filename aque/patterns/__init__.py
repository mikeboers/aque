import functools


registry = {}


def pattern(name, func=None):
    """Decorator to register a pattern.

    :param str name: Optional name for the pattern.
    
    ::
        @pattern
        def my_pattern(task):
            pass

        @pattern('explicit_name')
        def another_pattern(task):
            pass

    """

    if not isinstance(name, basestring):
        func = name
        name = func.__name__

    if not func:
        return functools.partial(pattern, name)

    registry[name] = func
    return func
