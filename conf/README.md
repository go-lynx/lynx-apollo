# Apollo Plugin Configuration

This directory contains configuration-related files for the Apollo plugin.

## Files

- `apollo.proto` - Protobuf definition for Apollo plugin configuration
- `apollo.pb.go` - Generated Go code from apollo.proto (run `make config` to regenerate)
- `defaults.go` - Default configuration values and constants
- `example_config.yml` - Example configuration file

## Generating Protobuf Files

To regenerate the protobuf Go code, run:

```bash
# From the lynx-apollo module root
make init
make config

# Or manually
PATH="$(go env GOPATH)/bin:$PATH" \
protoc --proto_path=./conf -I . -I ../lynx/third_party \
  --go_out=paths=source_relative:./conf \
  ./conf/apollo.proto
```

## Configuration Structure

The Apollo plugin configuration is defined in `apollo.proto` and includes:

- Basic configuration (app_id, cluster, namespace, meta_server)
- Authentication (token)
- Timeouts and intervals
- Feature flags (cache, metrics, retry, circuit breaker, etc.)
- Service configuration for multi-namespace loading

See `example_config.yml` for a complete configuration example.
See [`../VALIDATION.md`](../VALIDATION.md) for the current automated test baseline.
