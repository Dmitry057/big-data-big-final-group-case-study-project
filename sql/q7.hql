USE team30_projectdb;

DROP TABLE IF EXISTS q7_results;

CREATE TABLE q7_results
STORED AS PARQUET
AS
SELECT
    label,
    COUNT(*) AS connection_count,
    AVG(COALESCE(orig_pkts, 0)) AS avg_orig_pkts,
    AVG(COALESCE(resp_pkts, 0)) AS avg_resp_pkts,
    AVG(COALESCE(orig_pkts, 0) + COALESCE(resp_pkts, 0)) AS avg_total_pkts
FROM network_connections_part
GROUP BY label;
