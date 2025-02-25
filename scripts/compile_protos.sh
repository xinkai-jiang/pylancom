#!/bin/bash

# Get the directory of the script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROTO_DIR="$SCRIPT_DIR/../protos"
OUT_DIR="$SCRIPT_DIR/../pylancom/protos"

# Create the output directory if it doesn't exist
mkdir -p $OUT_DIR

# Compile the proto files
protoc --proto_path=$PROTO_DIR --python_out=$OUT_DIR $PROTO_DIR/*.proto

echo "Proto files compiled successfully and placed in $OUT_DIR"
