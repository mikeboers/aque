
registry = {}

def handler(func):
    registry[func.__name__] = func
    return func
