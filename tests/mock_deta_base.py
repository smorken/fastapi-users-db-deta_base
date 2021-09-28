from types import SimpleNamespace


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
                    if (
                        o["oauth_name"] == q["oauth_accounts.oauth_name"]
                        and o["account_id"] == q["oauth_accounts.account_id"]
                    ):
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
