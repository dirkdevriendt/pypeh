.PHONY: test-core test-dataframe test-rocrate test-all format

test-core:
	uv pip install -e ".[core, test-core]"
	uv run pytest -m core -W ignore tests/core

test-dataframe:
	uv pip install -e ".[dataframe-adapter, test-core]"
	uv run pytest -m dataframe -W ignore tests/dataframe_adapter

test-rocrate:
	uv pip install -e ".[rocrate-adapter, test-core]"
	uv run pytest -m rocrate -W ignore

test-all: test-core test-dataframe test-rocrate

format:
	uv pip install ruff
	uv run ruff format .