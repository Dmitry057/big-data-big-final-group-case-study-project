USE team30_projectdb;

DROP TABLE IF EXISTS q5_results;

CREATE TABLE q5_results
STORED AS PARQUET
AS
SELECT
    label,
    COALESCE(conn_state, 'UNKNOWN') AS conn_state,
    COUNT(*) AS row_count
FROM network_connections_part
GROUP BY label, COALESCE(conn_state, 'UNKNOWN')
ORDER BY label, conn_state;
