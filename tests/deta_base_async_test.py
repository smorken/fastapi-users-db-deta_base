import pytest
import asyncio
from types import SimpleNamespace
from fastapi_users_db_deta_base import deta_base_async


@pytest.mark.asyncio
async def test_looped_fetch_no_match():
    def mocked_fetch(query, last):
        if last is None:
            return SimpleNamespace(count=0, items=[], last=1)
        elif last == 1:
            return SimpleNamespace(count=0, items=[], last=None)

    loop = asyncio.get_event_loop()
    mocked_fetch_async = deta_base_async.wrap_async(loop, mocked_fetch)
    result1 = await deta_base_async.looped_fetch(mocked_fetch_async, {})
    assert result1 is None


@pytest.mark.asyncio
async def test_looped_fetch_single_match():
    def mocked_fetch(query, last):
        if last is None:
            return SimpleNamespace(count=0, items=[], last=1)
        elif last == 1:
            return SimpleNamespace(count=1, items=["match!"], last=2)
        elif last == 2:
            return SimpleNamespace(
                count=1, items=["2nd match"], last=None  # wont match
            )

    loop = asyncio.get_event_loop()
    mocked_fetch_async = deta_base_async.wrap_async(loop, mocked_fetch)
    result1 = await deta_base_async.looped_fetch(mocked_fetch_async, {})
    assert result1 == "match!"
