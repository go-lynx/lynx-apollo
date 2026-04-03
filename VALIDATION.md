# Validation

## Automated Baseline

Current workspace baseline:

```bash
go test ./...
```

Result summary:

```text
ok      github.com/go-lynx/lynx-apollo
?       github.com/go-lynx/lynx-apollo/conf    [no test files]
```

Supplemental static analysis:

```bash
go vet ./...
```

Result summary:

```text
ok
```

## Retained Semantics

- Omitted `circuit_breaker_threshold` is defaulted to `0.5` before validation so plugin validation and direct config validation stay consistent.
- The circuit breaker opens as soon as the closed-state failure rate reaches or exceeds the configured threshold.
- `ForceOpen()` starts the open-state cooldown from the moment it is called, so the next request is rejected until the recovery window elapses.
- Calls rejected while the breaker is already open do not change the recorded failure rate; only executed operations contribute to the counters.

## Recommended Manual Checks

- Start against a reachable Apollo Meta Server and verify `GetConfigValue()` returns expected values.
- Verify multi-namespace loading through `service_config.additional_namespaces`.
- Verify long-poll notifications through `ApolloConfigSource.Watch()` if your application depends on config watch integration.
