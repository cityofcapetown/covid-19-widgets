#!/usr/bin/env bash
set -e

DIRECOTRATE_PREFIX=$1
DIRECTORATE_NAME=$2

python3 ./hr-absence-layer-to-geojson.py "$DIRECOTRATE_PREFIX" "$DIRECTORATE_NAME"
