import redis

from .rpc import Container


class RPCNamespace(object):
    """
    Single element of an RPC path that makes a request when called.
    """
    def __init__(self, connection, path=None):
        self._connection = connection
        self._path = path or []

    def __getattr__(self, item):
        path = self._path[:]
        path.append(item.lower())
        return RPCNamespace(self._connection, path)

    def __call__(self, *args, **kwargs):
        return Container.call_remote(self._connection, '.'.join(self._path), *args)


class RPC(RPCNamespace):
    """
    RPC Client that allows recursive dot notation.

    Example:

          namespace.method(args)
    """
    def __init__(self, redis_url):
        super().__init__(redis.StrictRedis.from_url(redis_url))
