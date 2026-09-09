# ASAM ODS Interfaces

[ASAM-ODS-Interfaces](https://github.com/asam-ev/ASAM-ODS-Interfaces.git)

## Generation

For compatibility we need to make sure that the minimum required versions of the tools are installed
for stub generation. This makes sure that the stubs are compatible.
In python the stubs are [upwards compatible](https://protobuf.dev/support/cross-version-runtime-guarantee/#python).

Run `./.update_stubs.sh` to update stubs to the given version.

### Protoc compiler

``` sh
python -m pip install -U grpcio-tools
```

### ODS interfaces

``` sh
python -m grpc_tools.protoc --proto_path=. --python_out=. --pyi_out=. odsbox/proto/ods.proto odsbox/proto/ods_notification.proto odsbox/proto/ods_security.proto
```

### EXD API interfaces

We need to adjust the import of `ods_external_data.proto` to make it work in a python package.
Exchange `import "ods.proto";` by `import "odsbox/proto/ods.proto";`. Afterwards run the generator.

``` sh
!python -m grpc_tools.protoc --proto_path=. --python_out=. --pyi_out=. --grpc_python_out=. odsbox/proto/ods_external_data.proto
```
