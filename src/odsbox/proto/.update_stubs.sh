#!/usr/bin/env bash
# bash .update_stubs.sh

ASAM_ODS_VERSION="v6.2.1"

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

cd -- "${SCRIPT_DIR}"

# download current versions of the .proto files
curl -L -o ods.proto "https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/${ASAM_ODS_VERSION}/ods.proto"
curl -L -o ods_external_data.proto "https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/${ASAM_ODS_VERSION}/ods_external_data.proto"
curl -L -o ods_notification.proto "https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/${ASAM_ODS_VERSION}/ods_notification.proto"
curl -L -o ods_security.proto "https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/${ASAM_ODS_VERSION}/ods_security.proto"

# Convert imports to package path
sed -i 's/^import "ods.proto";$/import "odsbox\/proto\/ods.proto";/' ods_external_data.proto

cd -- "${PROJECT_ROOT}"

# make sure minimal dependency is used for generation
PROTOC_CMD=(
  uvx
  --python 3.12
  --from 'grpcio-tools==1.65.4'
  --with 'grpcio==1.65.4'
  --with 'protobuf==5.27.0'
  python -m grpc_tools.protoc
)

# Generate Python stubs
"${PROTOC_CMD[@]}" \
  -I. \
  --proto_path=. \
  --python_out=. \
  --pyi_out=. \
  odsbox/proto/ods.proto \
  odsbox/proto/ods_notification.proto \
  odsbox/proto/ods_security.proto

# Generate Python stubs including grpc
"${PROTOC_CMD[@]}" \
  -I. \
  --proto_path=. \
  --python_out=. \
  --pyi_out=. \
  --grpc_python_out=. \
  odsbox/proto/ods_external_data.proto

cd odsbox/proto
