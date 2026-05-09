#!/bin/bash

set -euo pipefail

spark_submit_bin=/usr/bin/spark-submit
python_bin=/usr/bin/python3

run_spark_job() {
    env -u VIRTUAL_ENV \
        PATH="/usr/bin:/bin" \
        HADOOP_CONF_DIR=/etc/hadoop/conf \
        YARN_CONF_DIR=/etc/hadoop/conf \
        PYSPARK_PYTHON="${python_bin}" \
        PYSPARK_DRIVER_PYTHON="${python_bin}" \
        "${spark_submit_bin}" \
        --master yarn \
        --num-executors 6 \
        --executor-cores 3 \
        --executor-memory 6G \
        --driver-memory 4G \
        "$@"
}

mkdir -p data models output

# Data preparation

run_spark_job scripts/ml_data_preparation.py

rm -rf data/train data/test
hdfs dfs -get project/data/train data/train
hdfs dfs -get project/data/test data/test

echo "Completed data preparation"


# Model training

run_spark_job scripts/ml_models_training.py

echo "Completed model 1 (RF) and model 2 (Logistic Regression) training"

rm -rf models/model1 models/model2
rm -f output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv

hdfs dfs -get project/models/model2 models/model2
hdfs dfs -get project/models/model1 models/model1
hdfs dfs -cat project/output/model1_predictions/*.csv > output/model1_predictions.csv
hdfs dfs -cat project/output/model2_predictions/*.csv > output/model2_predictions.csv

# Evaluation

run_spark_job scripts/ml_evaluation.py

hdfs dfs -cat project/output/evaluation/*.csv > output/evaluation.csv

echo "Completed evaluation"

