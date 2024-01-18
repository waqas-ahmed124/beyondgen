install-dev:
	pip install poetry
	poetry install --with dev
	pre-commit install

format:
	black .
