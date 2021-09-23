from types import SimpleNamespace


def wrap_async(loop, func):
    async def wrapped(*args, **kwargs):
        result = await loop.run_in_executor(
            None, func, *args, **kwargs)
        return result
    return wrapped


def wrap_deta_base_async(loop, deta_base):
    methods = ["put", "get", "delete", "insert", "put_many", "update", "fetch"]
    return SimpleNamespace(
        **{name: wrap_async(loop, getattr(deta_base, name))
           for name in methods})


async def looped_fetch(fetch, return_first_match=True, query=None):
    last = None
    result = []
    while True:
        res = await fetch(query, last)
        if res.count > 0:
            if return_first_match:
                return res.items[0]
            else:
                result.extend(res.items)
        last = res.last
        if not last:
            break
    return result if result else None
