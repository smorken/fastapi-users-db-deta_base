from typing import AsyncGenerator

import pytest
import asyncio

from fastapi_users_db_deta_base import DetaBaseUserDatabase
from fastapi_users_db_deta_base import deta_base_async
from tests.mock_deta_base import MockDetaBase
from tests.conftest import UserDB, UserDBOAuth


@pytest.fixture
async def deta_base_user_db() -> AsyncGenerator[DetaBaseUserDatabase, None]:
    loop = asyncio.get_event_loop()
    yield await deta_base_async.wrap_async(
        loop, lambda: DetaBaseUserDatabase(
            UserDB, deta_base_async.wrap_deta_base_async(
                loop, MockDetaBase())))()


@pytest.fixture
async def deta_base_user_db_oauth() -> AsyncGenerator[DetaBaseUserDatabase, None]:
    loop = asyncio.get_event_loop()
    yield await deta_base_async.wrap_async(
        loop, lambda: DetaBaseUserDatabase(
            UserDBOAuth, deta_base_async.wrap_deta_base_async(
                loop, MockDetaBase())))()


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries(deta_base_user_db: DetaBaseUserDatabase[UserDB]):
    user = UserDB(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
    )

    # Create
    user_db = await deta_base_user_db.create(user)
    assert user_db.id is not None
    assert user_db.is_active is True
    assert user_db.is_superuser is False
    assert user_db.email == user.email

    # Update
    user_db.is_superuser = True
    await deta_base_user_db.update(user_db)

    # Get by id
    id_user = await deta_base_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user_db.id
    assert id_user.is_superuser is True

    # Get by email
    email_user = await deta_base_user_db.get_by_email(str(user.email))
    assert email_user is not None
    assert email_user.id == user_db.id

    # Get by uppercased email
    email_user = await deta_base_user_db.get_by_email("Lancelot@camelot.bt")
    assert email_user is not None
    assert email_user.id == user_db.id

    # Exception when inserting existing email
    with pytest.raises(ValueError):
        await deta_base_user_db.create(user)

    # Unknown user
    unknown_user = await deta_base_user_db.get_by_email("galahad@camelot.bt")
    assert unknown_user is None

    # Delete user
    await deta_base_user_db.delete(user)
    deleted_user = await deta_base_user_db.get(user.id)
    assert deleted_user is None


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries_custom_fields(deta_base_user_db: DetaBaseUserDatabase[UserDB]):
    """It should output custom fields in query result."""
    user = UserDB(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
        first_name="Lancelot",
    )
    await deta_base_user_db.create(user)

    id_user = await deta_base_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user.id
    assert id_user.first_name == user.first_name


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries_oauth(
    deta_base_user_db_oauth: DetaBaseUserDatabase[UserDBOAuth],
    oauth_account1,
    oauth_account2,
):
    user = UserDBOAuth(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
        oauth_accounts=[oauth_account1, oauth_account2],
    )

    # Create
    user_db = await deta_base_user_db_oauth.create(user)
    assert user_db.id is not None
    assert hasattr(user_db, "oauth_accounts")
    assert len(user_db.oauth_accounts) == 2

    # Update
    user_db.oauth_accounts[0].access_token = "NEW_TOKEN"
    await deta_base_user_db_oauth.update(user_db)

    # Get by id
    id_user = await deta_base_user_db_oauth.get(user.id)
    assert id_user is not None
    assert id_user.id == user_db.id
    assert id_user.oauth_accounts[0].access_token == "NEW_TOKEN"

    # Get by email
    email_user = await deta_base_user_db_oauth.get_by_email(str(user.email))
    assert email_user is not None
    assert email_user.id == user_db.id
    assert len(email_user.oauth_accounts) == 2

    # Get by OAuth account
    oauth_user = await deta_base_user_db_oauth.get_by_oauth_account(
        oauth_account1.oauth_name, oauth_account1.account_id
    )
    assert oauth_user is not None
    assert oauth_user.id == user.id

    # Unknown OAuth account
    unknown_oauth_user = await deta_base_user_db_oauth.get_by_oauth_account("foo", "bar")
    assert unknown_oauth_user is None
