isort:
	isort ./fastapi_users_db_deta_base ./tests

format: isort
	black .

test:
	pytest --cov=fastapi_users_db_deta_base/ --cov-report=term-missing --cov-fail-under=100

bumpversion-major:
	bumpversion major

bumpversion-minor:
	bumpversion minor

bumpversion-patch:
	bumpversion patch
