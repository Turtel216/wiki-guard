.DEFAULT_GOAL := help

SBT := cd jvm && sbt --batch
UV := cd ml && uv run

# Where the Python side writes the model and scores that the JVM parity test reads.
export WG_PARITY_DIR := $(CURDIR)/jvm/target/parity

.PHONY: help
help: ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-15s %s\n", $$1, $$2}'

.PHONY: test
test: parity-fixture ## Run the Scala and Python unit tests
	$(UV) pytest
	$(SBT) test

.PHONY: parity-fixture
parity-fixture: ## Train a tiny model in Python for the JVM parity test
	$(UV) python -m wikiguard_ml.parity $(WG_PARITY_DIR)

.PHONY: fmt
fmt: ## Format and auto-fix Scala and Python code
	$(SBT) scalafixAll scalafmtAll scalafmtSbt
	$(UV) ruff check --fix .
	$(UV) ruff format .

.PHONY: lint
lint: ## Check formatting, lint rules and types without changing files
	$(SBT) scalafmtCheckAll scalafmtSbtCheck "scalafixAll --check"
	$(UV) ruff check .
	$(UV) ruff format --check .
	$(UV) mypy
