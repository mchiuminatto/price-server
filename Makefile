
.PHONY: lint lint-fix lint-check format format-check minikube-start redis-start redis-stop step1 step1-ohlc

# Use sudo for docker if the current user lacks socket access
DOCKER := $(shell docker info >/dev/null 2>&1 && echo docker || echo sudo docker)
DOCKER_COMPOSE := $(DOCKER) compose


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


# Start Minikube (installs if missing) and apply k8s manifests
minikube-start:
	bash scripts/minikube-start.sh

# Start Redis container (from docker-compose)
redis-start:
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml up -d redis

# Stop Redis container
redis-stop:
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml stop redis

# Run Step 1 only – Work Queue Serializer (tick data, default)
step1: redis-start
	bash scripts/run-step1.sh --data-type tick

# Run Step 1 only – Work Queue Serializer (ohlc data)
step1-ohlc: redis-start
	bash scripts/run-step1.sh --data-type ohlc

