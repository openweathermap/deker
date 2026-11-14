# Development commands for deker. Every command runs inside the uv-managed virtual environment.

PACKAGE      := deker
TESTS        := tests
DOCS         := docs/deker
PY_FILES      = $(wildcard $(shell git ls-files '*.py'))
TARGET_PYTHON := py310

.PHONY: help install format lint typecheck docstrings docs-lint check test coverage ci pre-commit clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Create the virtual environment and install all dependency groups
	uv sync --all-groups
	uv run pre-commit install --hook-type pre-commit --hook-type pre-push

format:  ## Format the code: ssort -> pyupgrade -> ruff (imports, autofixes, formatting)
	uv run ssort $(PACKAGE)
	-uv run pyupgrade --$(TARGET_PYTHON)-plus $(PY_FILES)  # exits 1 when it rewrites a file, that is not an error here
	-uv run ruff check --fix $(PACKAGE) $(TESTS)  # unfixable findings are reported by `make lint`, not here
	uv run ruff format $(PACKAGE) $(TESTS)

lint:  ## Lint the code and verify the formatting (ruff)
	uv run ruff check $(PACKAGE) $(TESTS)
	uv run ruff format --check $(PACKAGE) $(TESTS)

typecheck:  ## Static type check (mypy)
	uv run mypy $(PACKAGE) --install-types --non-interactive --config-file pyproject.toml

docstrings:  ## Check docstrings against signatures (pydoclint)
	uv run pydoclint --config=pyproject.toml $(PACKAGE)

docs-lint:  ## Lint the RST documentation (doc8)
	uv run doc8 --config=pyproject.toml $(DOCS)

check: lint typecheck docstrings docs-lint  ## Run every static check

test:  ## Run the tests in random order
	uv run pytest --random-order $(PYTEST_ARGS)

coverage:  ## Run the tests with coverage and enforce the threshold from pyproject.toml
	uv run pytest --random-order --cov=. tests/ --cov-report=term-missing --cov-report html

ci: format check coverage  ## Run every check and the tests sequentially, as CI does

pre-commit:  ## Run all pre-commit hooks on all files
	uv run pre-commit run --all-files

clean:  ## Remove caches and build artifacts
	rm -rf .ruff_cache .mypy_cache .pytest_cache .coverage dist build *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
