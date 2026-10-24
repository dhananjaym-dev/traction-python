"""The data plane's only charter concern: the abort skip-window marker, read fail-open."""

from tests.fakes import FakeRedis


def test_aborted_key():
    from traction.charters import aborted_key

    assert aborted_key("crm:py/contact-sync/run-1") == "tr:charter:crm:py/contact-sync/run-1:aborted"


async def test_is_aborted():
    from traction.charters import CharterAbortReader

    redis = FakeRedis()
    reader = CharterAbortReader(redis)
    assert not await reader.is_aborted("crm:py/g/run-1")
    redis.strings["tr:charter:crm:py/g/run-1:aborted"] = "operator"
    assert await reader.is_aborted("crm:py/g/run-1")


async def test_unreadable_marker_fails_open():
    from traction.charters import CharterAbortReader

    redis = FakeRedis()
    redis.fail.add("exists")
    assert not await CharterAbortReader(redis).is_aborted("crm:py/g/run-1")
