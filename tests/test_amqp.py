"""The AMQP layer: pamqp signed-int shim, wire message builder, confirmed publisher."""

import json

import pytest
from pamqp import decode, encode


def test_shim_encodes_negative_delivery_limit():
    import traction._amqp  # noqa: F401 — importing applies the shim

    table = encode.field_table({"x-delivery-limit": -1})
    assert b"x-delivery-limitI\xff\xff\xff\xff" in table


def test_shim_leaves_positive_ints_alone():
    import traction._amqp  # noqa: F401

    table = encode.field_table({"x-delivery-limit": 20})
    _, decoded = decode.field_table(table)
    assert decoded["x-delivery-limit"] == 20


def test_json_message_shape():
    from traction._amqp import TYPE_ID_REPORT, json_message

    message = json_message({"a": 1, "b": None}, TYPE_ID_REPORT)
    assert json.loads(message.body) == {"a": 1, "b": None}
    assert message.content_type == "application/json"
    assert message.headers["__TypeId__"] == "dev.traction.core.report.FlightReportMessage"
    assert message.delivery_mode == 2
    assert message.expiration is None


def test_json_message_expiration_ms():
    from traction._amqp import TYPE_ID_FLIGHT, json_message

    message = json_message({}, TYPE_ID_FLIGHT, expiration_ms=30_000)
    assert message.properties.expiration == "30000"


class FailingExchange:
    async def publish(self, message, routing_key, timeout=None):
        raise RuntimeError("broker said no")


class FakeChannel:
    default_exchange = FailingExchange()


async def test_confirmed_publisher_raises_on_failure():
    from traction._amqp import ConfirmedPublisher, PublishException

    publisher = ConfirmedPublisher(FakeChannel())
    with pytest.raises(PublishException):
        await publisher.publish("tr.tower.queue", {"x": 1}, "dev.traction.core.api.Flight")
