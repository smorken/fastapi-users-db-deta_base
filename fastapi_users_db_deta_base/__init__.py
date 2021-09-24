"""FastAPI Users database adapter for Deta Base."""
from typing import Optional, Type


from pydantic import UUID4

from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import UD
from fastapi_users_db_deta_base import deta_base_async
__version__ = "0.0.0"


class DetaBaseUserDatabase(BaseUserDatabase[UD]):
    """
    Database adapter for Deta Base.

    :param user_db_model: Pydantic model of a DB representation of a user.
    :param async_deta_base: deta base object with async method wrappers
    """

    def __init__(
        self,
        user_db_model: Type[UD],
        async_deta_base
    ):
        super().__init__(user_db_model)
        self.async_deta_base = async_deta_base

    async def get(self, id: UUID4) -> Optional[UD]:
        """Get a single user by id."""
        user = await self.async_deta_base.get(id)
        return self.user_db_model(**user) if user else None

    async def get_by_email(self, email: str) -> Optional[UD]:
        """Get a single user by email."""
        user = await deta_base_async.looped_fetch(
            self.async_deta_base.fetch,
            return_first_match=True,
            query={"email": email.lower()})

        return self.user_db_model(**user) if user else None

    async def get_by_oauth_account(self, oauth: str, account_id: str) -> Optional[UD]:
        """Get a single user by OAuth account id."""
        user = await deta_base_async.looped_fetch(
            self.async_deta_base.fetch,
            return_first_match=True,
            query={
                "oauth_accounts.oauth_name": oauth,
                "oauth_accounts.account_id": account_id,
            })

        return self.user_db_model(**user) if user else None

    async def create(self, user: UD) -> UD:
        """Create a user."""
        data = user.dict()
        data["email"] = data["email"].lower()
        if await self.get_by_email(data["email"]):
            raise ValueError()
        await self.async_deta_base.insert(
            data=user.dict(), key=user.id)
        return user

    async def update(self, user: UD) -> UD:
        """Update a user."""
        data = user.dict()
        data["email"] = data["email"].lower()
        await self.async_deta_base.put(
            data=user.dict(), key=user.id)
        return user

    async def delete(self, user: UD) -> None:
        """Delete a user."""
        await self.async_deta_base.delete(
            key=user.id)
