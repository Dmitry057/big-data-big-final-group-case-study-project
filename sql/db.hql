SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;
SET hive.resultset.use.unique.column.names=false;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;

DROP DATABASE IF EXISTS team30_projectdb CASCADE;
CREATE DATABASE team30_projectdb LOCATION 'project/hive/warehouse';
USE team30_projectdb;

dfs -rm -r -f project/hive/warehouse/captures_bucketed;
dfs -rm -r -f project/hive/warehouse/network_connections_part;

DROP TABLE IF EXISTS captures_ext;
CREATE EXTERNAL TABLE captures_ext (
    capture_id INT,
    capture_name STRING,
    source_file STRING,
    created_at STRING
)
STORED AS PARQUET
LOCATION 'project/warehouse/captures';

DROP TABLE IF EXISTS network_connections_ext;
CREATE EXTERNAL TABLE network_connections_ext (
    connection_id BIGINT,
    capture_id INT,
    ts STRING,
    uid STRING,
    id_orig_h STRING,
    id_orig_p INT,
    id_resp_h STRING,
    id_resp_p INT,
    proto STRING,
    service STRING,
    duration DOUBLE,
    orig_bytes BIGINT,
    resp_bytes BIGINT,
    conn_state STRING,
    local_orig BOOLEAN,
    local_resp BOOLEAN,
    missed_bytes BIGINT,
    history STRING,
    orig_pkts BIGINT,
    orig_ip_bytes BIGINT,
    resp_pkts BIGINT,
    resp_ip_bytes BIGINT,
    tunnel_parents STRING,
    label STRING,
    detailed_label STRING
)
STORED AS PARQUET
LOCATION 'project/warehouse/network_connections';

DROP TABLE IF EXISTS captures_bucketed;
CREATE EXTERNAL TABLE captures_bucketed (
    capture_id INT,
    capture_name STRING,
    source_file STRING,
    created_at TIMESTAMP
)
CLUSTERED BY (capture_id) INTO 4 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/captures_bucketed';

DROP TABLE IF EXISTS network_connections_part;
CREATE EXTERNAL TABLE network_connections_part (
    connection_id BIGINT,
    capture_id INT,
    ts TIMESTAMP,
    uid STRING,
    id_orig_h STRING,
    id_orig_p INT,
    id_resp_h STRING,
    id_resp_p INT,
    proto STRING,
    service STRING,
    duration DOUBLE,
    orig_bytes BIGINT,
    resp_bytes BIGINT,
    conn_state STRING,
    local_orig BOOLEAN,
    local_resp BOOLEAN,
    missed_bytes BIGINT,
    history STRING,
    orig_pkts BIGINT,
    orig_ip_bytes BIGINT,
    resp_pkts BIGINT,
    resp_ip_bytes BIGINT,
    tunnel_parents STRING,
    detailed_label STRING
)
PARTITIONED BY (label STRING)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/network_connections_part';

INSERT OVERWRITE TABLE captures_bucketed
SELECT capture_id, capture_name, source_file, CAST(created_at AS TIMESTAMP)
FROM captures_ext;

INSERT OVERWRITE TABLE network_connections_part PARTITION (label)
SELECT
    connection_id,
    capture_id,
    CAST(ts AS TIMESTAMP),
    uid,
    id_orig_h,
    id_orig_p,
    id_resp_h,
    id_resp_p,
    proto,
    service,
    duration,
    orig_bytes,
    resp_bytes,
    conn_state,
    local_orig,
    local_resp,
    missed_bytes,
    history,
    orig_pkts,
    orig_ip_bytes,
    resp_pkts,
    resp_ip_bytes,
    tunnel_parents,
    detailed_label,
    label
FROM network_connections_ext;

DROP TABLE IF EXISTS captures_ext;
DROP TABLE IF EXISTS network_connections_ext;

SHOW DATABASES;
USE team30_projectdb;
SHOW TABLES;
DESCRIBE FORMATTED captures_bucketed;
DESCRIBE FORMATTED network_connections_part;
SHOW PARTITIONS network_connections_part;
SELECT COUNT(*) AS captures_count FROM captures_bucketed;
SELECT COUNT(*) AS connections_count FROM network_connections_part;
SELECT label, COUNT(*) AS row_count
FROM network_connections_part
GROUP BY label
ORDER BY row_count DESC, label;
