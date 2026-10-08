.DEFAULT_GOAL := help

SBT := cd jvm && sbt --batch
UV := cd ml && uv run

# Where the Python side writes the model and scores that the JVM parity test reads.
export WG_PARITY_DIR := $(CURDIR)/jvm/target/parity

.PHONY: help
help: ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

.PHONY: test
test: parity-fixture ## Run the Scala and Python unit tests
	$(UV) pytest
	$(SBT) test

.PHONY: integration-test
integration-test: ## Run the tests that start containers (needs Docker)
	$(SBT) integration/test

.PHONY: parity-fixture
parity-fixture: ## Train a tiny model in Python for the JVM parity test
	$(UV) python -m wikiguard_ml.parity $(WG_PARITY_DIR)

.PHONY: dev-up
dev-up: ## Start the local dev cluster (needs Docker, k3d and kubectl)
	scripts/dev-up.sh

.PHONY: dev-down
dev-down: ## Delete the local dev cluster and its data
	scripts/dev-down.sh

.PHONY: lint-deploy
lint-deploy: ## Check the manifests under deploy/ (needs kubectl and kubeconform)
	scripts/validate-deploy.sh

.PHONY: prod-secrets
prod-secrets: ## Generate the encrypted production credentials (needs sops and age)
	scripts/prod-secrets.sh

.PHONY: fmt
fmt: ## Format and auto-fix Scala and Python code
	$(SBT) scalafixAll integration/scalafixAll scalafmtAll integration/scalafmtAll scalafmtSbt
	$(UV) ruff check --fix .
	$(UV) ruff format .

.PHONY: lint
lint: ## Check formatting, lint rules and types without changing files
	$(SBT) scalafmtCheckAll integration/scalafmtCheckAll scalafmtSbtCheck \
		"scalafixAll --check" "integration/scalafixAll --check"
	$(UV) ruff check .
	$(UV) ruff format --check .
	$(UV) mypy
