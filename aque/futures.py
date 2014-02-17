from threading import Condition
from concurrent.futures import _base




class Future(_base.Future):
    """A future which pulls results from the broker (Redis)."""

    def __init__(self, task):
        self.task = task

