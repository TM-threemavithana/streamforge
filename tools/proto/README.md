# Protobuf generation

The checked-in Go and Python bindings are generated from
`proto/streamforge/ingest/v1/ingestion.proto` with these pinned tools:

- `google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12`
- `google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2`
- `grpcio-tools==1.70.0`

The Python generator also supplies the bundled `protoc` compiler and the
well-known Google protobuf definitions. Regenerate both languages whenever the
canonical schema changes; do not edit generated files manually.
