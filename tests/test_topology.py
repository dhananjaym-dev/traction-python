"""Queue names and declare-time arguments — pinned to core/topology/Topology.java."""

from tests.fakes import FakeChannel


def test_queue_names():
    from traction.topology import DLQ, TOWER_QUEUE, queue_names

    names = queue_names("crm:py")
    assert names.airport_id == "crm:py"
    assert names.runway == "tr.crm:py.runway"
    assert names.holding == "tr.crm:py.holding"
    assert names.parked == "tr.crm:py.parked"
    assert TOWER_QUEUE == "tr.tower.queue"
    assert DLQ == "tr.dlq"


async def test_declare_airport_args_and_order():
    from traction.topology import declare_airport

    channel = FakeChannel()
    names = await declare_airport(channel, "crm:py")
    assert names.runway == "tr.crm:py.runway"

    declared = dict(channel.declared)
    # The DLX target is declared before the queue that dead-letters to it.
    assert [name for name, _ in channel.declared] == [
        "tr.crm:py.parked", "tr.crm:py.holding", "tr.crm:py.runway",
    ]
    assert declared["tr.crm:py.runway"] == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "tr.crm:py.parked",
        "x-delivery-limit": 20,
    }
    assert declared["tr.crm:py.holding"] == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "tr.crm:py.runway",
        "x-delivery-limit": -1,
    }
    assert declared["tr.crm:py.parked"] == {
        "x-queue-type": "quorum",
        "x-delivery-limit": -1,
    }


async def test_declare_tower_args_and_order():
    from traction.topology import declare_tower

    channel = FakeChannel()
    await declare_tower(channel)
    assert [name for name, _ in channel.declared] == ["tr.dlq", "tr.tower.queue"]
    declared = dict(channel.declared)
    assert declared["tr.dlq"] == {"x-queue-type": "quorum", "x-delivery-limit": -1}
    assert declared["tr.tower.queue"] == {
        "x-queue-type": "quorum",
        "x-delivery-limit": 20,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "tr.dlq",
    }
