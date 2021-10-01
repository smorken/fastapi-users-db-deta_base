"""FastAPI Users database adapter for Deta Base."""
from typing import Optional, Type


from pydantic import UUID4

from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import UD

__version__ = "0.0.0"


async def looped_fetch(fetch, query):
    """using the async fetch method of a deta Base instance return the first
    match of the specified query, or None if no match is found.

    :param fetch: async fetch method
    :param query: query object for which to fetch results
    :return: value matching the query or None
    """
    last = None
    while True:
        res = await fetch(query=query, last=last)
        if res.count > 0:
            return res.items[0]
        if not res.last:
            return None
        last = res.last


class DetaBaseUserDatabase(BaseUserDatabase[UD]):
    """
    Database adapter for Deta Base.

    :param user_db_model: Pydantic model of a DB representation of a user.
    :param async_deta_base: async deta base
    """

    def __init__(self, user_db_model: Type[UD], async_deta_base):
        super().__init__(user_db_model)
        self.async_deta_base = async_deta_base

    async def get(self, id: UUID4) -> Optional[UD]:
        """Get a single user by id."""
        user = await self.async_deta_base.get(key=id)
        return self.user_db_model(**user) if user else None

    async def get_by_email(self, email: str) -> Optional[UD]:
        """Get a single user by email."""
        user = await looped_fetch(
            self.async_deta_base.fetch, query={"email": email.lower()}
        )

        return self.user_db_model(**user) if user else None

    async def get_by_oauth_account(self, oauth: str, account_id: str) -> Optional[UD]:
        """Get a single user by OAuth account id."""
        user = await looped_fetch(
            self.async_deta_base.fetch,
            query={
                "oauth_accounts.oauth_name": oauth,
                "oauth_accounts.account_id": account_id,
            },
        )

        return self.user_db_model(**user) if user else None

    async def create(self, user: UD) -> UD:
        """Create a user."""
        data = user.dict()
        data["email"] = data["email"].lower()
        if await self.get_by_email(data["email"]):
            raise ValueError()
        await self.async_deta_base.insert(data=user.dict(), key=user.id)
        return user

    async def update(self, user: UD) -> UD:
        """Update a user."""
        data = user.dict()
        data["email"] = data["email"].lower()
        await self.async_deta_base.put(data=user.dict(), key=user.id)
        return user

    async def delete(self, user: UD) -> None:
        """Delete a user."""
        await self.async_deta_base.delete(key=user.id)
