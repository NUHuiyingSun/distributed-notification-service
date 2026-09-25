.DEFAULT_GOAL := help
MODULES := notification-api notification-dispatcher notification-worker

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Compile and run unit tests
	mvn -B -ntp verify

infra: ## Start Kafka, Redis and LocalStack
	docker compose up -d --wait

up: ## Start infrastructure + all services in Docker
	docker compose --profile apps up -d --build --wait

down: ## Stop everything and remove volumes
	docker compose --profile apps --profile tools down -v

tools: ## Start Kafka UI on http://localhost:8090
	docker compose --profile tools up -d

run: ## Run the three services locally in the background (logs in ./logs)
	mvn -B -ntp -q -DskipTests package
	@mkdir -p logs
	@for m in $(MODULES); do \
		nohup java -jar $$m/target/$$m-*.jar > logs/$$m.log 2>&1 & echo "started $$m"; \
	done
	./scripts/wait-for-services.sh

stop: ## Stop services started with `make run`
	-pkill -f 'notification-(api|dispatcher|worker)-.*\.jar'

e2e: ## Run end-to-end checks (worker must run with SIMULATED_FAILURE_RATE=0)
	./scripts/e2e-test.sh

.PHONY: help build infra up down tools run stop e2e
