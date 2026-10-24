"""In-memory fakes for the store and broker seams. Each records enough for assertions and can
be told to fail, so error paths are as testable as happy paths."""

from __future__ import annotations

import json
from typing import Any


class FakeRedisError(ConnectionError):
    pass


class FakeRedis:
    def __init__(self):
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.set_calls: list[tuple[str, str, int | None]] = []
        self.hset_calls: list[tuple[str, dict[str, str]]] = []
        self.scripts: dict[str, str] = {}
        self.evalsha_results: list[Any] = []  # queued results; an Exception instance raises
        self.evalsha_calls: list[tuple[str, list[str], list[str]]] = []
        self.fail: set[str] = set()  # method names that raise FakeRedisError

    def _check(self, method: str) -> None:
        if method in self.fail:
            raise FakeRedisError(method)

    async def get(self, key):
        self._check("get")
        return self.strings.get(key)

    async def set(self, key, value, px=None, ex=None):
        self._check("set")
        self.strings[key] = str(value)
        self.set_calls.append((key, str(value), px if px is not None else ex))

    async def exists(self, key):
        self._check("exists")
        return int(key in self.strings or key in self.hashes)

    async def hget(self, key, field):
        self._check("hget")
        return self.hashes.get(key, {}).get(field)

    async def hset(self, key, mapping):
        self._check("hset")
        self.hashes.setdefault(key, {}).update({k: str(v) for k, v in mapping.items()})
        self.hset_calls.append((key, dict(mapping)))

    async def script_load(self, script):
        self._check("script_load")
        sha = f"sha-{len(self.scripts)}"
        self.scripts[sha] = script
        return sha

    async def evalsha(self, sha, numkeys, *keys_and_args):
        self._check("evalsha")
        keys = list(keys_and_args[:numkeys])
        args = list(keys_and_args[numkeys:])
        self.evalsha_calls.append((sha, keys, args))
        if not self.evalsha_results:
            raise AssertionError("no queued evalsha result")
        result = self.evalsha_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class FakeMessage:
    """An aio-pika-shaped published message, as FakeExchange records it."""

    def __init__(self, message, routing_key):
        self.routing_key = routing_key
        self.body = message.body
        self.headers = dict(message.headers or {})
        self.expiration = getattr(message.properties, "expiration", None)

    @property
    def json(self):
        return json.loads(self.body)


class FakeExchange:
    def __init__(self):
        self.published: list[FakeMessage] = []
        self.fail_next = 0  # raise on the next N publishes

    async def publish(self, message, routing_key, timeout=None):
        if self.fail_next > 0:
            self.fail_next -= 1
            raise RuntimeError("publish failed")
        self.published.append(FakeMessage(message, routing_key))


class FakeChannel:
    def __init__(self):
        self.default_exchange = FakeExchange()
        self.declared: list[tuple[str, dict]] = []

    async def declare_queue(self, name, durable=True, arguments=None):
        assert durable is True
        self.declared.append((name, dict(arguments or {})))


class FakeQueue:
    def __init__(self, name):
        self.name = name
        self.consumers: dict[str, Any] = {}
        self.cancelled: list[str] = []
        self._tags = 0

    async def consume(self, callback):
        self._tags += 1
        tag = f"ctag-{self._tags}"
        self.consumers[tag] = callback
        return tag

    async def cancel(self, tag):
        self.cancelled.append(tag)
        self.consumers.pop(tag, None)


class FakeRegistryChannel(FakeChannel):
    def __init__(self, publisher_confirms=True):
        super().__init__()
        self.publisher_confirms = publisher_confirms
        self.qos: int | None = None
        self.queues: dict[str, FakeQueue] = {}
        self.closed = False

    async def set_qos(self, prefetch_count):
        self.qos = prefetch_count

    async def get_queue(self, name):
        return self.queues.setdefault(name, FakeQueue(name))

    async def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.channels: list[FakeRegistryChannel] = []

    async def channel(self, publisher_confirms=True):
        channel = FakeRegistryChannel(publisher_confirms)
        self.channels.append(channel)
        return channel


class FakeIncomingMessage:
    """One delivery under manual ack."""

    def __init__(self, body: bytes | dict, headers: dict | None = None):
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.headers = headers or {}
        self.content_type = "application/json"
        self.acks = 0
        self.nacks: list[bool] = []  # requeue flags

    async def ack(self):
        self.acks += 1

    async def nack(self, requeue=True):
        self.nacks.append(requeue)


class FakePublisher:
    """Stands in for ConfirmedPublisher."""

    def __init__(self):
        self.published: list[tuple[str, dict, str, int | None]] = []
        self.fail_next = 0

    async def publish(self, queue, body, type_id, expiration_ms=None):
        from traction._amqp import PublishException

        if self.fail_next > 0:
            self.fail_next -= 1
            raise PublishException(f"no confirm for {queue}")
        self.published.append((queue, body, type_id, expiration_ms))
