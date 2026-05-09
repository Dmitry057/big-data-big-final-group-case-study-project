#!/bin/bash

set -euo pipefail

mkdir -p data
rm -f data/*.csv
rm -rf data/google_drive_download

if ! command -v gdown >/dev/null 2>&1; then
    echo "gdown is required in .venv for dataset download." >&2
    exit 1
fi

gdown --folder \
    "https://drive.google.com/drive/folders/1c_1Ex4Y2brRMk-qy7UG15nVZwSEoBDWP?usp=sharing" \
    -O data/google_drive_download

find data/google_drive_download -type f -name '*.csv' -exec mv {} data/ \;
rm -rf data/google_drive_download

echo "Dataset collection finished."
echo "Files are available in data/"
