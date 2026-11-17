# Development commands for deker. Every command runs inside the uv-managed virtual environment.

PACKAGE      := deker
TESTS        := tests
DOCS         := docs/deker
PY_FILES      = $(wildcard $(shell git ls-files '*.py'))
TARGET_PYTHON := py310
# the supported Pythons and the next one, for test-matrix
PYTHONS      ?= 3.10 3.11 3.12 3.13 3.14 3.15
# the lowest NumPy of pyproject.toml on every supported Python (an old NumPy has no wheels for a new Python), as
# python:numpy pairs, for test-numpy-floor; CI tests the same pairs (numpy-floors of the workflows)
NUMPY_FLOORS ?= 3.10:1.26.4 3.11:1.26.4 3.12:1.26.4 3.13:2.1.0 3.14:2.3.2
# the sibling packages of [tool.uv.sources], built as wheels for install-matrix until they are released
SIBLINGS     := ../deker-local-adapters ../deker-server-adapters ../deker-tools

.PHONY: help install format lint typecheck docstrings docs-lint check test test-matrix test-numpy-floor install-matrix coverage ci pre-commit clean

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

test-matrix:  ## Run the tests on every Python of PYTHONS in a temporary environment each, like tox
	@for py in $(PYTHONS); do \
	  echo "=== Python $$py"; \
	  uv run --isolated --python $$py pytest --random-order $(PYTEST_ARGS) || exit 1; \
	done

test-numpy-floor:  ## Run the tests on the lowest supported NumPy of every Python of NUMPY_FLOORS, in a temporary environment each, as CI does
	@for pair in $(NUMPY_FLOORS); do \
	  py=$${pair%%:*}; np=$${pair#*:}; \
	  echo "=== Python $$py, NumPy $$np"; \
	  tmp=$$(mktemp -d); \
	  uv venv --quiet --python $$py $$tmp/venv \
	  && uv pip install --quiet --python $$tmp/venv/bin/python --editable . --group dev "numpy==$$np" \
	  && $$tmp/venv/bin/python -c "import importlib.metadata as m; v = {d.metadata['Name'].lower(): d.version for d in m.distributions()}; print(*(f'{p} {v[p]}' for p in ('numpy', 'pandas', 'xarray', 'h5py') if p in v))" \
	  && $$tmp/venv/bin/python -m pytest --random-order $(PYTEST_ARGS); \
	  status=$$?; rm -rf $$tmp; [ $$status -eq 0 ] || exit $$status; \
	done

install-matrix:  ## Check the package installs from wheels only, as a user without a compiler would, on every Python of PYTHONS
	@tmp=$$(mktemp -d); \
	for src in . $(SIBLINGS); do uv build --wheel --quiet --out-dir $$tmp/dist $$src || exit 1; done; \
	for py in $(PYTHONS); do \
	  if uv venv --quiet --python $$py $$tmp/venv-$$py >$$tmp/log 2>&1 \
	     && uv pip install --quiet --python $$tmp/venv-$$py/bin/python --only-binary :all: $$tmp/dist/*.whl >$$tmp/log 2>&1; then \
	    echo "Python $$py: installs from wheels"; \
	  else \
	    missing=$$( { grep -oE "Distribution \`[^\`]+\`" $$tmp/log | sed -E 's/Distribution `([^@ ]+).*/\1/'; \
	                  grep -oE "[A-Za-z0-9_.-]+[^ ]* has no usable wheels" $$tmp/log | sed -E 's/ has no usable wheels//'; } | sort -u | tr '\n' ' '); \
	    if [ -n "$$missing" ]; then echo "Python $$py: no wheel for $$missing- a compiler would be needed"; \
	    else echo "Python $$py: cannot be installed"; fi; \
	    sed 's/^/    /' $$tmp/log; \
	    rm -rf $$tmp; exit 1; \
	  fi; \
	done; rm -rf $$tmp

coverage:  ## Run the tests with coverage and enforce the threshold from pyproject.toml
	uv run pytest --random-order --cov=. tests/ --cov-report=term-missing --cov-report html $(PYTEST_ARGS)

ci: format check coverage  ## Run every check and the tests sequentially, as CI does

pre-commit:  ## Run all pre-commit hooks on all files
	uv run pre-commit run --all-files

clean:  ## Remove caches and build artifacts
	rm -rf .ruff_cache .mypy_cache .pytest_cache .coverage dist build *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
