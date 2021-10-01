from types import SimpleNamespace
from functools import partial


def _wrap_async(loop, func):
    """Wraps the specified function as an async function

    :param loop: event loop to pass to asyncio.run_in_executor
    :param func: function to wrap
    """

    async def wrapped(*args, **kwargs):
        result = await loop.run_in_executor(None, partial(func, *args, **kwargs))
        return result

    return wrapped


def _wrap_deta_base_async(loop, deta_base):
    """Wraps a deta Base object replacing each of the Base methods with
    async methods.

    :param loop: event loop to pass to asyncio.run_in_executor
    :param deta_base: a deta Base instance
    :return: object mimicing deta Base, but with async methods
    """
    methods = ["put", "get", "delete", "insert", "put_many", "update", "fetch"]
    return SimpleNamespace(
        **{name: _wrap_async(loop, getattr(deta_base, name)) for name in methods}
    )


def get_async_mock_deta_base(loop):
    return _wrap_deta_base_async(loop, MockDetaBase())


class MockDetaBase:
    def __init__(self):
        self._data_by_key = {}

    def put(self, data, key):
        self._data_by_key[key] = data

    def get(self, key):
        if key in self._data_by_key:
            return self._data_by_key[key]
        return None

    def delete(self, key):
        self._data_by_key.pop(key, None)

    def insert(self, data, key):
        if key in self._data_by_key:
            raise ValueError()
        self.put(data, key)

    def put_many(self, items):
        raise NotImplementedError()

    def update(self, updates, key):
        raise NotImplementedError()

    def fetch(self, query=None, limit=1000, last=None):
        def match_email(v, query):
            return "email" in query and v["email"] == query["email"]

        def match_oauth(v, q):
            if "oauth_accounts.oauth_name" in q:
                for o in v["oauth_accounts"]:
                    oauth_match = o["oauth_name"] == q["oauth_accounts.oauth_name"]
                    oauth_match &= o["account_id"] == q["oauth_accounts.account_id"]
                    if oauth_match:
                        return True
            return False

        match = None
        for v in self._data_by_key.values():
            if match_email(v, query) or match_oauth(v, query):
                match = v
                break

        return SimpleNamespace(
            count=1 if match else 0, last=None, items=[match] if match else []
        )
