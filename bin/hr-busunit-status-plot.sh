#!/usr/bin/env bash
set -e

DIRECTORATE_PREFIX=$1
DIRECTORATE_TITLE=$2

python3 ./ct_covid_hr_busunit_status_widget.py "$DIRECTORATE_PREFIX" "$DIRECTORATE_TITLE"