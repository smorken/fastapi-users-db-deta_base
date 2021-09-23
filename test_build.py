# flake8: noqa
import sys

try:
    from fastapi_users_db_deta_base import DetaBaseUserDatabase
except:
    sys.exit(1)

sys.exit(0)
