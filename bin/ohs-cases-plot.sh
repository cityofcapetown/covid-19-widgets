#!/usr/bin/env bash
set -e

DIRECTORATE_PREFIX=$1
DIRECTORATE_TITLE=$2

python3 ./ohs_cases_plot_to_minio.py "$DIRECTORATE_PREFIX" "$DIRECTORATE_TITLE"
