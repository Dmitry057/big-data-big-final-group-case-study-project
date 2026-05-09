#!/bin/bash
#
# Optional manual benchmark script.
# This is not part of stage1.sh or main.sh.
# Run it separately only when you want benchmark numbers for the report.

set -euo pipefail

password=$(head -n 1 secrets/.psql.pass)

spark_submit_bin=/usr/bin/spark-submit
python_bin=/usr/bin/python3

# run_spark_benchmark() {
#     local format="$1"
#     local hdfs_path="$2"
#     local mode="$3"
#     local -a spark_args

#     spark_args=(--master yarn)

#     if [ "${format}" = "avro" ]; then
#         spark_args+=(--packages com.databricks:spark-avro_2.11:4.0.0)
#     fi

#     env -u VIRTUAL_ENV \
#         PATH="/usr/bin:/bin" \
#         HADOOP_CONF_DIR=/etc/hadoop/conf \
#         YARN_CONF_DIR=/etc/hadoop/conf \
#         PYSPARK_PYTHON="${python_bin}" \
#         PYSPARK_DRIVER_PYTHON="${python_bin}" \
#         "${spark_submit_bin}" \
#         "${spark_args[@]}" \
#         scripts/benchmark_storage_read.py \
#         --format "${format}" \
#         --path "${hdfs_path}" \
#         --mode "${mode}"
# }

# mkdir -p output
# echo "format,codec,run,write_seconds,hdfs_bytes,full_scan_seconds,analytic_seconds" > output/stage1_storage_benchmark_runs.csv

# benchmark_pairs=(
#     "avro:snappy"
#     "avro:bzip2"
#     "parquet:gzip"
#     "parquet:snappy"
# )

# for benchmark_pair in "${benchmark_pairs[@]}"; do
#     IFS=":" read -r format codec <<< "${benchmark_pair}"

#     for run in 1 2 3; do
#         benchmark_name="${format}_${codec}_run${run}"
#         hdfs_path="/user/team30/project/benchmark/${benchmark_name}"

#         echo "Benchmarking ${benchmark_name}"
#         hdfs dfs -rm -r -f "${hdfs_path}" || true

#         start_time=$(date +%s)

#         if [ "${format}" = "avro" ]; then
#             format_flag="--as-avrodatafile"
#         else
#             format_flag="--as-parquetfile"
#         fi

#         sqoop import \
#             --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team30_projectdb \
#             --username team30 \
#             --password "${password}" \
#             --compression-codec "${codec}" \
#             --compress \
#             ${format_flag} \
#             --target-dir "${hdfs_path}" \
#             --class-name "network_connections_${format}_${codec}_run${run}" \
#             --m 1 \
#             --query "SELECT connection_id, capture_id, ts, uid, CAST(id_orig_h AS varchar(64)) AS id_orig_h, id_orig_p, CAST(id_resp_h AS varchar(64)) AS id_resp_h, id_resp_p, proto, service, duration, orig_bytes, resp_bytes, conn_state, local_orig, local_resp, missed_bytes, history, orig_pkts, orig_ip_bytes, resp_pkts, resp_ip_bytes, tunnel_parents, label, detailed_label FROM network_connections WHERE connection_id <= 100000 AND \$CONDITIONS"

#         end_time=$(date +%s)
#         write_seconds=$((end_time - start_time))
#         hdfs_bytes=$(hdfs dfs -du -s "${hdfs_path}" | awk '{print $1}')

#         set +e
#         full_scan_output=$(run_spark_benchmark "${format}" "${hdfs_path}" full_scan 2>&1)
#         full_scan_status=$?
#         set -e
#         full_scan_json=$(printf '%s\n' "${full_scan_output}" | "${python_bin}" -c 'import sys
# lines = [line for line in sys.stdin.read().splitlines() if line.startswith("BENCHMARK_RESULT=")]
# print(lines[-1].split("=", 1)[1] if lines else "")
# ')
#         if [ "${full_scan_status}" -ne 0 ] || [ -z "${full_scan_json}" ]; then
#             echo "Spark full_scan benchmark failed for ${benchmark_name}" >&2
#             echo "${full_scan_output}" >&2
#             exit 1
#         fi

#         set +e
#         analytic_output=$(run_spark_benchmark "${format}" "${hdfs_path}" analytic 2>&1)
#         analytic_status=$?
#         set -e
#         analytic_json=$(printf '%s\n' "${analytic_output}" | "${python_bin}" -c 'import sys
# lines = [line for line in sys.stdin.read().splitlines() if line.startswith("BENCHMARK_RESULT=")]
# print(lines[-1].split("=", 1)[1] if lines else "")
# ')
#         if [ "${analytic_status}" -ne 0 ] || [ -z "${analytic_json}" ]; then
#             echo "Spark analytic benchmark failed for ${benchmark_name}" >&2
#             echo "${analytic_output}" >&2
#             exit 1
#         fi

#         full_scan_seconds=$(echo "${full_scan_json}" | "${python_bin}" -c 'import json,sys; print(json.loads(sys.stdin.read())["elapsed_seconds"])')
#         analytic_seconds=$(echo "${analytic_json}" | "${python_bin}" -c 'import json,sys; print(json.loads(sys.stdin.read())["elapsed_seconds"])')

#         echo "${format},${codec},${run},${write_seconds},${hdfs_bytes},${full_scan_seconds},${analytic_seconds}" >> output/stage1_storage_benchmark_runs.csv
#     done
# done

"${python_bin}" - <<'PY'
import csv
from collections import defaultdict

input_path = "output/stage1_storage_benchmark_runs.csv"
output_path = "output/stage1_storage_benchmark_avg.csv"
groups = defaultdict(lambda: {
    "write_seconds": [],
    "hdfs_bytes": [],
    "full_scan_seconds": [],
    "analytic_seconds": [],
})

with open(input_path, newline="", encoding="utf-8") as source:
    reader = csv.DictReader(source)
    for row in reader:
        key = (row["format"], row["codec"])
        groups[key]["write_seconds"].append(float(row["write_seconds"]))
        groups[key]["hdfs_bytes"].append(float(row["hdfs_bytes"]))
        groups[key]["full_scan_seconds"].append(float(row["full_scan_seconds"]))
        groups[key]["analytic_seconds"].append(float(row["analytic_seconds"]))

with open(output_path, "w", newline="", encoding="utf-8") as target:
    writer = csv.writer(target)
    writer.writerow([
        "format",
        "codec",
        "runs",
        "avg_write_seconds",
        "avg_hdfs_bytes",
        "avg_full_scan_seconds",
        "avg_analytic_seconds",
    ])

    for (file_format, codec), metrics in sorted(groups.items()):
        writer.writerow([
            file_format,
            codec,
            len(metrics["write_seconds"]),
            sum(metrics["write_seconds"]) / len(metrics["write_seconds"]),
            sum(metrics["hdfs_bytes"]) / len(metrics["hdfs_bytes"]),
            sum(metrics["full_scan_seconds"]) / len(metrics["full_scan_seconds"]),
            sum(metrics["analytic_seconds"]) / len(metrics["analytic_seconds"]),
        ])
PY

echo "Storage benchmark finished: output/stage1_storage_benchmark_runs.csv"
echo "Storage benchmark averages: output/stage1_storage_benchmark_avg.csv"
