USE team30_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE TABLE q1_results
STORED AS PARQUET
AS
SELECT
    label,
    COUNT(*) AS row_count,
    CAST(COUNT(*) AS DOUBLE) / SUM(COUNT(*)) OVER () AS share_fraction
FROM network_connections_part
GROUP BY label;
