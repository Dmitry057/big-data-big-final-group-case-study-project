#!/bin/bash

set -euo pipefail

password=$(head -n 1 secrets/.psql.pass)

echo "Cleaning local generated artifacts"
rm -f ./*.avsc
rm -f ./*.java

echo "Cleaning HDFS warehouse directory"
hdfs dfs -rm -r -f /user/team30/project/warehouse || true
hdfs dfs -mkdir -p /user/team30/project/warehouse

mkdir -p output/sqoop_artifacts
rm -f output/sqoop_artifacts/*.java
rm -f output/sqoop_artifacts/*.avsc

echo "Listing PostgreSQL tables before import"
sqoop list-tables \
    --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team30_projectdb \
    --username team30 \
    --password "${password}"

echo "Importing captures to HDFS in Parquet with Snappy compression"
sqoop import \
    --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team30_projectdb \
    --username team30 \
    --password "${password}" \
    --compression-codec snappy \
    --compress \
    --as-parquetfile \
    --target-dir /user/team30/project/warehouse/captures \
    --class-name captures_sqoop \
    --outdir output/sqoop_artifacts \
    --m 1 \
    --query "SELECT capture_id, capture_name, source_file, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS created_at FROM captures WHERE \$CONDITIONS"

echo "Importing network_connections to HDFS in Parquet with Snappy compression"
sqoop import \
    --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team30_projectdb \
    --username team30 \
    --password "${password}" \
    --compression-codec snappy \
    --compress \
    --as-parquetfile \
    --target-dir /user/team30/project/warehouse/network_connections \
    --class-name network_connections_sqoop \
    --outdir output/sqoop_artifacts \
    --m 1 \
    --query "SELECT connection_id, capture_id, to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS ts, uid, CAST(id_orig_h AS varchar(64)) AS id_orig_h, id_orig_p, CAST(id_resp_h AS varchar(64)) AS id_resp_h, id_resp_p, proto, service, duration, orig_bytes, resp_bytes, conn_state, local_orig, local_resp, missed_bytes, history, orig_pkts, orig_ip_bytes, resp_pkts, resp_ip_bytes, tunnel_parents, label, detailed_label FROM network_connections WHERE \$CONDITIONS"

find . -maxdepth 1 -type f -name '*.avsc' -exec mv {} output/sqoop_artifacts/ \;
find . -maxdepth 1 -type f -name '*.java' -exec mv {} output/sqoop_artifacts/ \;

echo "Sqoop import finished."
echo "HDFS warehouse: /user/team30/project/warehouse"
echo "Local Sqoop artifacts: output/sqoop_artifacts"
