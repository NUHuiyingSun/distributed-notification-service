# Contributing

Thanks for your interest! Issues and pull requests are welcome.

## Development setup

Prerequisites: JDK 17+, Maven 3.9+, Docker with Compose v2, `jq` (for the end-to-end script).

```bash
make build                                 # compile + unit tests
make infra                                 # Kafka, Redis, LocalStack
SIMULATED_FAILURE_RATE=0 make run          # start the three services locally
make e2e                                   # end-to-end checks
make stop && make down                     # clean up
```

## Guidelines

- Keep each service focused on its stage of the pipeline (see [docs/DESIGN.md](docs/DESIGN.md)).
- Any change to delivery semantics (retry, idempotency, DLQ handling) must come with a unit test
  and, where it affects observable behavior, an assertion in `scripts/e2e-test.sh`.
- Redis key layout lives only in `RedisKeys`; status transitions only in `NotificationStatus`.
- Follow the existing code style: constructor injection, records for data, no field injection.
- Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `test:` …).

## Pull requests

1. Fork and create a branch from `main`.
2. Make sure `mvn verify` and the CI end-to-end job pass.
3. Describe *what* changed and *why* in the PR template.
