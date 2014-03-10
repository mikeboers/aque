import os
import urlparse

from aque.utils import decode_callable
from aque.brokers.base import Broker


def get_broker(url=None):

    if isinstance(url, Broker):
        return url
    
    url = url or os.environ.get('AQUE_BROKER', 'memory:')
    parts = urlparse.urlsplit(url)
    cls = decode_callable(parts.scheme, 'aque_brokers')
    if not cls:
        raise ValueError('no broker for %s' % parts.scheme)
    return cls.from_url(parts)
