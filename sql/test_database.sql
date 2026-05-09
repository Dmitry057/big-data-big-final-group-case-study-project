SELECT COUNT(*) AS capture_count FROM captures;

SELECT COUNT(*) AS connection_count FROM network_connections;

SELECT capture_id, source_file
FROM captures
LIMIT 5;

SELECT *
FROM network_connections
LIMIT 5;
