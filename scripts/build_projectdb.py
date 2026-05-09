#!/usr/bin/env python3

"""Build and populate the team30 PostgreSQL database for Stage 1."""

import re
from pathlib import Path
from pprint import pprint

import psycopg2


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
SECRETS_FILE = ROOT_DIR / "secrets" / ".psql.pass"
CREATE_TABLES_SQL = ROOT_DIR / "sql" / "create_tables.sql"
IMPORT_DATA_SQL = ROOT_DIR / "sql" / "import_data.sql"
TEST_DATABASE_SQL = ROOT_DIR / "sql" / "test_database.sql"

DB_HOST = "hadoop-04.uni.innopolis.ru"
DB_PORT = "5432"
DB_USER = "team30"
DB_NAME = "team30_projectdb"

CAPTURE_FILE_PATTERN = re.compile(
    r"^CTU-IoT-Malware-Capture-(?P<capture_id>\d+)-1conn\.log\.labeled\.csv$"
)

INSERT_CAPTURE_SQL = """
INSERT INTO captures (capture_id, capture_name, source_file)
VALUES (%s, %s, %s)
ON CONFLICT (capture_id) DO UPDATE
SET capture_name = EXCLUDED.capture_name,
    source_file = EXCLUDED.source_file;
"""

TRANSFORM_AND_INSERT_SQL = """
INSERT INTO network_connections (
    capture_id,
    ts,
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
    label,
    detailed_label
)
SELECT
    %s AS capture_id,
    to_timestamp(ts_raw::double precision),
    uid,
    NULLIF(id_orig_h, '-')::inet,
    NULLIF(REGEXP_REPLACE(id_orig_p, '\.0+$', ''), '-')::integer,
    NULLIF(id_resp_h, '-')::inet,
    NULLIF(REGEXP_REPLACE(id_resp_p, '\.0+$', ''), '-')::integer,
    proto,
    NULLIF(service, '-'),
    NULLIF(duration, '-')::double precision,
    NULLIF(REGEXP_REPLACE(orig_bytes, '\.0+$', ''), '-')::bigint,
    NULLIF(REGEXP_REPLACE(resp_bytes, '\.0+$', ''), '-')::bigint,
    NULLIF(conn_state, '-'),
    CASE
        WHEN local_orig IN ('T', 't', 'true', 'TRUE', '1') THEN true
        WHEN local_orig IN ('F', 'f', 'false', 'FALSE', '0') THEN false
        ELSE NULL
    END,
    CASE
        WHEN local_resp IN ('T', 't', 'true', 'TRUE', '1') THEN true
        WHEN local_resp IN ('F', 'f', 'false', 'FALSE', '0') THEN false
        ELSE NULL
    END,
    NULLIF(REGEXP_REPLACE(missed_bytes, '\.0+$', ''), '-')::bigint,
    NULLIF(history, '-'),
    NULLIF(REGEXP_REPLACE(orig_pkts, '\.0+$', ''), '-')::bigint,
    NULLIF(REGEXP_REPLACE(orig_ip_bytes, '\.0+$', ''), '-')::bigint,
    NULLIF(REGEXP_REPLACE(resp_pkts, '\.0+$', ''), '-')::bigint,
    NULLIF(REGEXP_REPLACE(resp_ip_bytes, '\.0+$', ''), '-')::bigint,
    NULLIF(tunnel_parents, '-'),
    REGEXP_REPLACE(BTRIM(label), '\s+', ' ', 'g'),
    NULLIF(
        NULLIF(REGEXP_REPLACE(BTRIM(detailed_label), '\s+', ' ', 'g'), ''),
        '-'
    )
FROM stg_network_connections;
"""


def read_password() -> str:
    """Read the PostgreSQL password from the local secrets file."""

    return SECRETS_FILE.read_text(encoding="utf-8").strip()


def load_sql(path: Path) -> str:
    """Load a SQL file from disk."""

    return path.read_text(encoding="utf-8")


def list_dataset_files() -> list[Path]:
    """Return sorted dataset files from the repository data directory."""

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV files were found in data/")
    return csv_files


def parse_capture_metadata(csv_path: Path) -> tuple[int, str]:
    """Extract capture id and capture name from the dataset filename."""

    match = CAPTURE_FILE_PATTERN.match(csv_path.name)
    if match is None:
        raise ValueError(f"Unexpected dataset file name: {csv_path.name}")

    capture_id = int(match.group("capture_id"))
    capture_name = f"CTU-IoT-Malware-Capture-{capture_id}"
    return capture_id, capture_name


def build_connection_string(password: str) -> str:
    """Create the psycopg2 connection string."""

    return (
        f"host={DB_HOST} "
        f"port={DB_PORT} "
        f"user={DB_USER} "
        f"dbname={DB_NAME} "
        f"password={password}"
    )


def execute_validation_queries(connection: psycopg2.extensions.connection) -> None:
    """Run validation queries and print the results."""

    commands = [
        command.strip()
        for command in load_sql(TEST_DATABASE_SQL).split(";")
        if command.strip()
    ]

    with connection.cursor() as cursor:
        for command in commands:
            cursor.execute(command)
            pprint({"query": command, "rows": cursor.fetchall()})


def main() -> None:
    """Build tables and load all dataset files."""

    password = read_password()
    create_tables_sql = load_sql(CREATE_TABLES_SQL)
    copy_command = load_sql(IMPORT_DATA_SQL).strip()
    csv_files = list_dataset_files()

    with psycopg2.connect(build_connection_string(password)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(create_tables_sql)
        connection.commit()

        for csv_file in csv_files:
            capture_id, capture_name = parse_capture_metadata(csv_file)
            print(f"Loading {csv_file.name} into capture {capture_id}")

            with connection.cursor() as cursor:
                cursor.execute(INSERT_CAPTURE_SQL, (capture_id, capture_name, csv_file.name))
                cursor.execute("TRUNCATE TABLE stg_network_connections")

                with csv_file.open("r", encoding="utf-8", newline="") as source_file:
                    cursor.copy_expert(copy_command, source_file)

                cursor.execute(TRANSFORM_AND_INSERT_SQL, (capture_id,))

            connection.commit()

        with connection.cursor() as cursor:
            cursor.execute("ANALYZE captures")
            cursor.execute("ANALYZE network_connections")
        connection.commit()

        execute_validation_queries(connection)


if __name__ == "__main__":
    main()
