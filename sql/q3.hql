USE team30_projectdb;

DROP TABLE IF EXISTS q3_results;

CREATE TABLE q3_results
STORED AS PARQUET
AS
SELECT
    label,
    COUNT(*) AS connection_count,
    AVG(COALESCE(orig_bytes, 0)) AS avg_orig_bytes,
    AVG(COALESCE(resp_bytes, 0)) AS avg_resp_bytes,
    AVG(COALESCE(orig_bytes, 0) + COALESCE(resp_bytes, 0)) AS avg_total_bytes
FROM network_connections_part
GROUP BY label;
