.PHONY: lint lint-fix lint-check format format-check

PIPELINE_DIRS := pipeline/ tests/pipeline/
VENV        := .venv
RUFF        := $(VENV)/bin/ruff

# Check for violations (exit non-zero on any finding)
lint:
	$(RUFF) check $(PIPELINE_DIRS)

# Auto-fix safe violations in-place
lint-fix:
	$(RUFF) check --fix $(PIPELINE_DIRS)

# Alias: same as lint (useful in CI)
lint-check: lint

# Check formatting without modifying files
format-check:
	$(RUFF) format --check $(PIPELINE_DIRS)

# Apply formatting in-place
format:
	$(RUFF) format $(PIPELINE_DIRS)
