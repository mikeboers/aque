from concurrent.futures import _base


class Future(_base.Future):
    """A future which pulls results from the broker (Redis)."""

    def __init__(self, broker, id):
        super(Future, self).__init__()
        self.id = id
        self.broker = broker
        self.dependencies = []
