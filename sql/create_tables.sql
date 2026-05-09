BEGIN;

SET TIME ZONE 'UTC';

DROP TABLE IF EXISTS network_connections CASCADE;
DROP TABLE IF EXISTS captures CASCADE;

CREATE TABLE captures (
    capture_id integer PRIMARY KEY,
    capture_name varchar(128) NOT NULL,
    source_file varchar(255) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS network_connections (
    connection_id bigint GENERATED ALWAYS AS IDENTITY,
    capture_id integer NOT NULL,
    ts timestamptz NOT NULL,
    uid varchar(64) NOT NULL,
    id_orig_h inet NOT NULL,
    id_orig_p integer,
    id_resp_h inet NOT NULL,
    id_resp_p integer,
    proto varchar(16) NOT NULL,
    service varchar(32),
    duration double precision,
    orig_bytes bigint,
    resp_bytes bigint,
    conn_state varchar(16),
    local_orig boolean,
    local_resp boolean,
    missed_bytes bigint,
    history varchar(64),
    orig_pkts bigint,
    orig_ip_bytes bigint,
    resp_pkts bigint,
    resp_ip_bytes bigint,
    tunnel_parents text,
    label varchar(64) NOT NULL,
    detailed_label varchar(128),
    CONSTRAINT pk_network_connections PRIMARY KEY (connection_id),
    CONSTRAINT fk_network_connections_capture
        FOREIGN KEY (capture_id) REFERENCES captures (capture_id)
);

CREATE TEMP TABLE IF NOT EXISTS stg_network_connections (
    ts_raw text,
    uid text,
    id_orig_h text,
    id_orig_p text,
    id_resp_h text,
    id_resp_p text,
    proto text,
    service text,
    duration text,
    orig_bytes text,
    resp_bytes text,
    conn_state text,
    local_orig text,
    local_resp text,
    missed_bytes text,
    history text,
    orig_pkts text,
    orig_ip_bytes text,
    resp_pkts text,
    resp_ip_bytes text,
    tunnel_parents text,
    label text,
    detailed_label text
) ON COMMIT PRESERVE ROWS;

COMMIT;
