USE team30_projectdb;

DROP TABLE IF EXISTS q6_results;

CREATE TABLE q6_results
STORED AS PARQUET
AS
SELECT
    label,
    id_resp_p,
    COUNT(*) AS row_count
FROM network_connections_part
WHERE label <> 'Benign'
    AND id_resp_p IS NOT NULL
GROUP BY label, id_resp_p
ORDER BY row_count DESC, label, id_resp_p;
