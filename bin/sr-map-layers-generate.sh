#!/usr/bin/env bash
set -e

DIRECTORATE_PREFIX=$1
DIRECTORATE_TITLE=$2

python3 ./service_request_map_layers_to_minio.py "$DIRECTORATE_PREFIX" "$DIRECTORATE_TITLE"