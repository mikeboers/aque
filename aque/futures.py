from concurrent.futures import _base


class Future(_base.Future):
    """A future which pulls results from the broker (Redis)."""

    def __init__(self, broker, id):
        super(Future, self).__init__()
        self.id = id
        self.broker = broker

    def iter(self):
        return self._iter(set())

    def _iter(self, visited):

        if self.id in visited:
            return
        visited.add(self.id)
        yield self

        for dep_id in self.broker.fetch(self.id).get('dependencies', ()):
            dep = self.broker.get_future(dep_id)
            for x in dep._iter(visited):
                yield x
