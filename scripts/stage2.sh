#!/bin/bash

set -euo pipefail

password=$(head -n 1 secrets/.hive.pass)
beeline_bin=/usr/local/bin/beeline

mkdir -p output
rm -f output/q*.csv

run_hive_file() {
    local hql_file="$1"
    local output_file="$2"

    "${beeline_bin}" \
        -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
        -n team30 \
        -p "${password}" \
        -f "${hql_file}" \
        > "${output_file}" \
        2>/dev/null
}

export_hive_table_to_csv() {
    local table_name="$1"
    local output_file="$2"

    "${beeline_bin}" \
        --silent=true \
        --showHeader=true \
        --outputformat=csv2 \
        -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
        -n team30 \
        -p "${password}" \
        -e "SET hive.execution.engine=tez; SET hive.resultset.use.unique.column.names=false; SET hive.vectorized.execution.enabled=false; SET hive.vectorized.execution.reduce.enabled=false; USE team30_projectdb; SELECT * FROM ${table_name};" \
        > "${output_file}" \
        2>/dev/null
}

echo "Hive tables creation"

run_hive_file sql/db.hql output/hive_results.txt

echo "EDA"

for query_id in 1 2 3 4 5 6 7; do
    echo "Running Stage 2 query q${query_id}"
    run_hive_file "sql/q${query_id}.hql" "output/q${query_id}_hive.txt"
    export_hive_table_to_csv "q${query_id}_results" "output/q${query_id}.csv"
done

echo "Stage 2 finished"
