from concurrent.futures import _base


class Future(_base.Future):
    """A future which pulls results from the broker (Redis)."""

    def __init__(self, broker, id, children=None, dependencies=None):
        super(Future, self).__init__()
        self.id = id
        self.broker = broker
        self.children = children or []
        self.dependencies = dependencies or []


