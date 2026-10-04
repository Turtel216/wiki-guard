.DEFAULT_GOAL := help

SBT := cd jvm && sbt --batch
UV := cd ml && uv run

.PHONY: help
help: ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

.PHONY: test
test: ## Run the Scala and Python unit tests
	$(SBT) test
	$(UV) pytest

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
