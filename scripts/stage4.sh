#!/bin/bash

set -euo pipefail

password=$(head -n 1 secrets/.hive.pass)
beeline_bin=/usr/local/bin/beeline

mkdir -p output

required_outputs=(
    "output/model1_predictions.csv"
    "output/model2_predictions.csv"
    "output/evaluation.csv"
)

for output_file in "${required_outputs[@]}"; do
    if [ ! -s "${output_file}" ]; then
        echo "Missing required Stage 3 output: ${output_file}" >&2
        exit 1
    fi
done

run_hive_file() {
    local hql_file="$1"
    local output_file="$2"

    if ! env -u VIRTUAL_ENV \
        PATH="/usr/bin:/bin" \
        "${beeline_bin}" \
        -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
        -n team30 \
        -p "${password}" \
        -f "${hql_file}" \
        > "${output_file}" \
        2> "${output_file}.err"; then
        echo "Hive failed for ${hql_file}; see ${output_file}.err" >&2
        return 1
    fi
}

export_hive_table_to_csv() {
    local table_name="$1"
    local output_file="$2"

    if ! env -u VIRTUAL_ENV \
        PATH="/usr/bin:/bin" \
        "${beeline_bin}" \
        --silent=true \
        --showHeader=true \
        --outputformat=csv2 \
        -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
        -n team30 \
        -p "${password}" \
        -e "SET hive.execution.engine=tez; SET hive.resultset.use.unique.column.names=false; SET hive.vectorized.execution.enabled=false; SET hive.vectorized.execution.reduce.enabled=false; USE team30_projectdb; SELECT * FROM ${table_name};" \
        > "${output_file}" \
        2> "${output_file}.err"; then
        echo "Export failed for ${table_name}; see ${output_file}.err" >&2
        return 1
    fi
}

echo "Creating Stage 4 Hive tables for Superset"
run_hive_file sql/stage4_tables.hql output/stage4_hive_tables.txt

echo "Exporting compact Stage 4 dashboard datasets"
export_hive_table_to_csv stage4_model_evaluation output/stage4_model_evaluation.csv
export_hive_table_to_csv stage4_prediction_counts output/stage4_prediction_counts.csv
export_hive_table_to_csv stage4_prediction_distribution output/stage4_prediction_distribution.csv
export_hive_table_to_csv stage4_feature_extraction_summary output/stage4_feature_extraction_summary.csv
export_hive_table_to_csv stage4_hyperparameter_results output/stage4_hyperparameter_results.csv

echo "Stage 4 data preparation finished"
 