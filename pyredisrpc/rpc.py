import functools
import time
import redis
import msgpack
import uuid
import inspect

from .logger import logger

from multiprocessing import Queue, Process, Lock


WORKERS = 4
REGISTRY = {}

RETURN_PRIMITIVES = (bytes, int, float, tuple, list, str, dict, type(None))


class ArgumentException(Exception):
    """Invalid arguments for RPC method"""
    pass


class RPCUnhandledException(Exception):
    """An exception occurred that isn't an RPC exception"""
    pass


class RPCInvalidReturnException(Exception):
    """Method returned a value that isn't a valid primative"""
    pass


class RPCDoesNotExist(Exception):
    """Method does not exist"""
    pass


class RPCInvalidCall(Exception):
    """RPC payload is corrupt or can't be decoded"""
    pass


class RPCResultException(Exception):
    """Result contains exception that occurred in remote RPC worker"""
    pass


class RPCMessage(object):
    def __init__(self, packed=None, task_id=None, method=None, args=None):
        self.task_id = task_id
        self.method = method
        self.args = args or []

        if packed is not None:
            data = msgpack.loads(packed)
            logger.debug('Unpacking message {}'.format(data))
            try:
                self.task_id = data[b'task_id'].decode('utf-8')
                self.method = data[b'method'].decode('utf-8')
                self.args = []

                for arg in data[b'args']:
                    if isinstance(arg, bytes):
                        arg = arg.decode('utf-8')
                    self.args.append(arg)
            except KeyError:
                raise RPCInvalidCall('Could not unpack invalid message')

    def serialize(self):
        return msgpack.dumps({
            'task_id': self.task_id,
            'method': self.method,
            'args': self.args
        })


class RPCResult(object):
    def __init__(self, packed):
        self._raw = packed
        self._data = msgpack.loads(packed)

        logger.debug('RPCResult: {}'.format(self._data))

    @property
    def success(self):
        return self._data[b'success']

    @property
    def type(self):
        return self._data[b'type']

    @property
    def value(self):
        if self.success:
            value = self._data[b'result']
        else:
            value = self._data[b'error']

        if isinstance(value, bytes):
            return value.decode('utf-8')

        return value

    def raise_exception(self):
        if not self.success:
            raise RPCResultException(self.value)


class Future(object):
    """Returned by client method to allow client to sit and wait for response"""
    def __init__(self, connection, task_id):
        self._result_key = 'result:{}'.format(task_id)
        self._result = None
        self.redis = connection
        self.task_id = task_id

    def wait(self):
        if self._result is None:
            result = self.redis.get(self._result_key)
            if result is not None:
                self._result = RPCResult(result)
        if self._result is not None:
            return
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self._result_key)
        for item in pubsub.listen():
            if item['type'] != 'subscribe':
                self._result = RPCResult(item['data'])
                pubsub.close()

    def result(self, raise_exception=True):
        if self._result is not None:
            if raise_exception:
                self._result.raise_exception()
            return self._result
        self.wait()
        if raise_exception:
            self._result.raise_exception()
        return self._result.value

    def info(self):
        return self._result


def rpc(f):
    """
    Decorator that registered Python class methods as RPC

    RPC has the signature:
        namespace{class.__name__}.method{f.__name__}

    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        sig = inspect.signature(f)
        if len(args) != len(sig.parameters.keys()):
            raise ArgumentException('Method has invalid number of parameters')
        try:
            return f(*args, **kwargs)
        except Exception as exc:
            raise RPCUnhandledException(exc)
    wrapper.__add_rpc = True
    return wrapper


def rpc_worker(idx, container, lock, q):
    """Worker for queued items containing valid msgpack data"""

    while True:
        lock.acquire()
        try:
            task = q.get(True)
        finally:
            lock.release()

        message = RPCMessage(task)

        logger.info('Worker {} invoke {} {}'.format(idx, message.method, message.task_id))

        start = time.time()

        container.call_real_and_resolve(message)

        end = time.time()

        logger.debug(
            'Worker {} finished {} {} {} seconds'.format(idx, message.method, message.task_id, end - start)
        )

        container.redis.lrem('working', 0, task)


class Service(object):
    """Represents a service"""
    def __init__(self, name=None):
        self.name = name or self.__class__.__name__


class Container(object):
    """Entry point for one or more Service objects and responsible for executing n workers"""
    def __init__(self, services, msgpack_default=None, redis_url=None, connection=None):
        self.services = services
        self.msgpack_default = msgpack_default

        if self.msgpack_default is None:
            self.msgpack_default = lambda x: x

        if redis_url:
            self.redis = redis.StrictRedis.from_url(redis_url)
        elif connection:
            self.redis = connection
        else:
            self.redis = self.redis = redis.StrictRedis.from_url('redis:///')

    def run(self):
        for service in self.services:
            for name in dir(service):
                if callable(getattr(service, name)) and hasattr(getattr(service, name), '__add_rpc'):
                    method_address = '{}.{}'.format(service.name.lower(), name)
                    REGISTRY[method_address] = getattr(service, name)

        logger.info(
            '\nRegistered service addresses:\n\n{}\n'.format(
                '\n'.join([' * {}'.format(k) for k in REGISTRY.keys()]))
        )

        lock = Lock()
        q = Queue(maxsize=WORKERS)
        running = []

        for idx in range(WORKERS):
            ps = Process(target=rpc_worker, args=(idx, self, lock, q))
            ps.start()
            running.append(ps)

            logger.info('Started worker {}'.format(idx))

        while True:
            task = self.redis.brpoplpush('rpc', 'working', 30)

            if task:
                q.put(task)

            for idx, ps in enumerate(running):
                if not ps.is_alive():
                    logger.warning('Worker {} died, restarting'.format(idx))
                    running[idx] = Process(target=rpc_worker, args=(idx, self, lock, q)).start()

    def call_real(self, message):
        """Call RPC method directly and return result"""
        logger.debug('Call inbound {} with {}'.format(message.method, message.args))

        try:
            try:
                func = REGISTRY[message.method]
            except KeyError:
                raise RPCDoesNotExist('Method does not exist: {}'.format(message.method))

            try:
                result = self.msgpack_default(func(*message.args))

                if not isinstance(result, RETURN_PRIMITIVES):
                    raise RPCInvalidReturnException(
                        'Method returned invalid objects type. '
                        'Must be a primitive type of:\n{}'
                            .format(', '.join([type(p()).__name__ for p in RETURN_PRIMITIVES]))
                    )

                return {
                    'success': True,
                    'result': result,
                    'type': type(result).__name__
                }
            except Exception as exc:
                if exc is Exception:
                    raise RPCUnhandledException(exc)
                raise

        except (Exception, RPCUnhandledException) as exc:
            logger.error(exc)
            return {
                'success': False,
                'error': str(exc),
                'type': type(exc).__name__
            }

    def call_real_and_resolve(self, message):
        """Call RPC directly and resolve results to orginating clients"""
        result = msgpack.dumps(self.call_real(message), default=self.msgpack_default)
        self.redis.setex('result:{}'.format(message.task_id), 3600, result)
        self.redis.publish('result:{}'.format(message.task_id), result)
        return result

    @staticmethod
    def call_remote(connection, rpc_name, *args):
        """Convenience factory to make low level call to RPC"""
        logger.debug('Call outbound: {} {}'.format(rpc_name, args))

        message = RPCMessage(
            task_id=str(uuid.uuid4()),
            method=rpc_name,
            args=args
        )

        connection.lpush('rpc', message.serialize())

        return Future(connection, message.task_id)
