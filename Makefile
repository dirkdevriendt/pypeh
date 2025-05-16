.PHONY: test-core test-dataframe test-rocrate test-all format

test-core:
	uv pip install -e ".[core,test-core]"
	uv run pytest -m core -W ignore

test-dataframe:
	uv pip install -e ".[dataframe-adapter,test-core,test-dataframe]"
	uv run pytest -m dataframe -W ignore

test-rocrate:
	uv pip install -e ".[rocrate-adapter,test-core,test-rocrate]"
	uv run pytest -m rocrate -W ignore

test-all: test-core test-dataframe test-rocrate

format:
	uv pip install ruff
	uv run ruff format .