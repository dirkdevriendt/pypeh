.PHONY: test-core test-dataframe test-rocrate test-all format

test-core:
	uv pip install -e ".[core, test-core]"
	uv run pytest tests -m core --disable-warnings

test-dataframe:
	uv pip install -e ".[pandera-adapter, test-core]"
	uv run pytest -s -vv tests/adapters tests/core/interfaces -m dataframe --disable-warnings

test-rocrate:
	uv pip install -e ".[rocrate-adapter, test-core]"
	uv run pytest -m rocrate -W ignore

test-s3:
	uv pip install -e ".[s3-adapter, test-s3]"
	uv run pytest -m s3 -W ignore

test-all: test-core test-dataframe test-rocrate

format:
	uv pip install ruff
	uv run ruff format .