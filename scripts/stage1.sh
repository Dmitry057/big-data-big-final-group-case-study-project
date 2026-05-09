#!/bin/bash

set -euo pipefail

source .venv/bin/activate

echo "Collecting the Malware Detection dataset into data/"
bash scripts/data_collection.sh

echo "Building team30 PostgreSQL database"
python scripts/build_projectdb.py

echo "Importing PostgreSQL tables to HDFS with Sqoop"
bash scripts/import_to_hdfs.sh

echo "Stage 1 finished."
