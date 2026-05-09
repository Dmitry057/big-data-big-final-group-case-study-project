USE team30_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE TABLE q2_results
STORED AS PARQUET
AS
SELECT
    label,
    proto,
    COUNT(*) AS row_count
FROM network_connections_part
GROUP BY label, proto;
