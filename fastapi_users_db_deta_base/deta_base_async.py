from types import SimpleNamespace
from functools import partial


def wrap_async(loop, func):
    """Wraps the specified function as an async function

    :param loop: event loop to pass to asyncio.run_in_executor
    :param func: function to wrap
    """

    async def wrapped(*args, **kwargs):
        result = await loop.run_in_executor(None, partial(func, *args, **kwargs))
        return result

    return wrapped


def wrap_deta_base_async(loop, deta_base):
    """Wraps a deta Base object replacing each of the Base methods with
    async methods.

    :param loop: event loop to pass to asyncio.run_in_executor
    :param deta_base: a deta Base instance
    :return: object mimicing deta Base, but with async methods
    """
    methods = ["put", "get", "delete", "insert", "put_many", "update", "fetch"]
    return SimpleNamespace(
        **{name: wrap_async(loop, getattr(deta_base, name)) for name in methods}
    )


async def looped_fetch(fetch, query):
    """using the async fetch method of a deta Base instance return the first
    match of the specified query, or None if no match is found.

    :param fetch: async fetch method
    :param query: query object for which to fetch results
    :return: value matching the query or None
    """
    last = None
    while True:
        res = await fetch(query, last)
        if res.count > 0:
            return res.items[0]
        if not last:
            return None
        last = res.last
