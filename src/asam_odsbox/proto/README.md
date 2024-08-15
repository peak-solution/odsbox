# ASAM ODS Interfaces

[ASAM-ODS-Interfaces](https://github.com/asam-ev/ASAM-ODS-Interfaces.git)

``` sh
python -m pip install -U grpcio-tools
python -m grpc_tools.protoc --proto_path=. --python_out=proto/. --pyi_out=proto/. *.proto
```