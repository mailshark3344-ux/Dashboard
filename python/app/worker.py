import os
import time
import csv
import json
from datetime import datetime, timezone
from io import StringIO

import boto3
import psycopg2
from psycopg2.extras import Json
from botocore.exceptions import ClientError, EndpointConnectionError


# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://host.docker.internal:9000"
).strip()

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin"
).strip()

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin123"
).strip()

MINIO_REGION = os.getenv(
    "MINIO_REGION",
    "us-east-1"
).strip()

MINIO_SECURE = (
    os.getenv(
        "MINIO_SECURE",
        "false"
    ).strip().lower()
    in (
        "true",
        "1",
        "yes",
        "y",
        "on"
    )
)

# ------------------------------------------------------------
# NORMALIZE ENDPOINT
# ------------------------------------------------------------

if not MINIO_ENDPOINT.startswith(
    "http://"
) and not MINIO_ENDPOINT.startswith(
    "https://"
):

    protocol = (
        "https"
        if MINIO_SECURE
        else "http"
    )

    MINIO_ENDPOINT = (
        f"{protocol}://"
        f"{MINIO_ENDPOINT}"
    )


MINIO_ENDPOINT = MINIO_ENDPOINT.rstrip("/")


# ============================================================
# BUCKET CONFIGURATION
# ============================================================

MINIO_BUCKETS_RAW = os.getenv(
    "MINIO_BUCKETS",
    ""
).strip()

LEGACY_MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    ""
).strip()

LEGACY_DEFAULT_BUCKET = os.getenv(
    "LEGACY_DEFAULT_BUCKET",
    LEGACY_MINIO_BUCKET or "myfiles"
).strip()


# ============================================================
# PREFIX
# ============================================================

MINIO_PREFIX = os.getenv(
    "MINIO_PREFIX",
    ""
).strip()


# ============================================================
# WORKER CONFIGURATION
# ============================================================

POLL_INTERVAL = int(
    os.getenv(
        "POLL_INTERVAL",
        "10"
    )
)

FORCE_REPROCESS = (
    os.getenv(
        "FORCE_REPROCESS",
        "false"
    ).strip().lower()
    in (
        "true",
        "1",
        "yes",
        "y",
        "on"
    )
)


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "postgres"
).strip()

POSTGRES_PORT = int(
    os.getenv(
        "POSTGRES_PORT",
        "5432"
    )
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "redash"
).strip()

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "redash"
).strip()

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "redashpass"
)


# ============================================================
# SUPPORTED FILE TYPES
# ============================================================

SUPPORTED_EXTENSIONS = (
    ".csv",
    ".json",
    ".jsonl",
    ".ndjson",
)


# ============================================================
# PRINT CONFIGURATION
# ============================================================

def print_configuration():

    print(
        "",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    print(
        "CDC PYTHON MINIO WORKER",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    print(
        f"MinIO endpoint : {MINIO_ENDPOINT}",
        flush=True
    )

    print(
        f"MinIO region   : {MINIO_REGION}",
        flush=True
    )

    print(
        f"MinIO secure   : {MINIO_SECURE}",
        flush=True
    )

    print(
        f"Access key     : "
        f"{'*' * len(MINIO_ACCESS_KEY)}",
        flush=True
    )

    configured_buckets = get_configured_buckets()

    if configured_buckets:

        print(
            "Bucket mode    : EXPLICIT",
            flush=True
        )

        print(
            "Buckets        : "
            + ", ".join(
                configured_buckets
            ),
            flush=True
        )

    else:

        print(
            "Bucket mode    : AUTO DISCOVERY",
            flush=True
        )

        print(
            "Buckets        : ALL ACCESSIBLE BUCKETS",
            flush=True
        )

    print(
        f"MinIO prefix   : "
        f"{MINIO_PREFIX or '(entire bucket)'}",
        flush=True
    )

    print(
        f"PostgreSQL     : "
        f"{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/"
        f"{POSTGRES_DB}",
        flush=True
    )

    print(
        f"Poll interval  : {POLL_INTERVAL} seconds",
        flush=True
    )

    print(
        "Supported files: CSV / JSON / JSONL / NDJSON",
        flush=True
    )

    print(
        f"Force reprocess: {FORCE_REPROCESS}",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )


# ============================================================
# BUCKET CONFIGURATION
# ============================================================

def get_configured_buckets():

    buckets = []

    if MINIO_BUCKETS_RAW:

        for bucket in MINIO_BUCKETS_RAW.split(","):

            bucket = bucket.strip()

            if (
                bucket
                and bucket not in buckets
            ):

                buckets.append(
                    bucket
                )

    elif LEGACY_MINIO_BUCKET:

        buckets.append(
            LEGACY_MINIO_BUCKET
        )

    return buckets


# ============================================================
# MINIO CLIENT
# ============================================================

def create_minio_client():

    print(
        "",
        flush=True
    )

    print(
        "Creating MinIO/S3 client...",
        flush=True
    )

    print(
        f"Endpoint: {MINIO_ENDPOINT}",
        flush=True
    )

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
        use_ssl=MINIO_SECURE,
    )

    return client


s3 = create_minio_client()


# ============================================================
# TEST MINIO CONNECTION
# ============================================================

def test_minio_connection():

    print(
        "",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    print(
        "TESTING MINIO CONNECTION",
        flush=True
    )

    print(
        f"Endpoint : {MINIO_ENDPOINT}",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    try:

        response = s3.list_buckets()

        buckets = []

        for bucket in response.get(
            "Buckets",
            []
        ):

            name = bucket.get(
                "Name"
            )

            if name:

                buckets.append(
                    name
                )

        buckets.sort()

        print(
            "MINIO CONNECTION SUCCESS",
            flush=True
        )

        print(
            f"Accessible buckets: {len(buckets)}",
            flush=True
        )

        for bucket in buckets:

            print(
                f"  - {bucket}",
                flush=True
            )

        print(
            "============================================================",
            flush=True
        )

        return True

    except EndpointConnectionError as e:

        print(
            "MINIO CONNECTION FAILED",
            flush=True
        )

        print(
            "Cannot connect to the MinIO endpoint.",
            flush=True
        )

        print(
            f"Endpoint: {MINIO_ENDPOINT}",
            flush=True
        )

        print(
            f"Error: {e}",
            flush=True
        )

        print(
            "",
            flush=True
        )

        print(
            "Check that:",
            flush=True
        )

        print(
            "  1. MinIO is running.",
            flush=True
        )

        print(
            "  2. Port 9000 is reachable.",
            flush=True
        )

        print(
            "  3. MINIO_ENDPOINT is correct.",
            flush=True
        )

        print(
            "  4. Docker can reach the MinIO server.",
            flush=True
        )

        return False

    except ClientError as e:

        print(
            "MINIO AUTHENTICATION / AUTHORIZATION FAILED",
            flush=True
        )

        print(
            f"Endpoint: {MINIO_ENDPOINT}",
            flush=True
        )

        print(
            f"Error: {e}",
            flush=True
        )

        print(
            "",
            flush=True
        )

        print(
            "Check MINIO_ACCESS_KEY and MINIO_SECRET_KEY.",
            flush=True
        )

        return False

    except Exception as e:

        print(
            "MINIO CONNECTION FAILED",
            flush=True
        )

        print(
            f"Error: {e}",
            flush=True
        )

        return False


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_db_connection():

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def initialize_database():

    conn = get_db_connection()

    try:

        cursor = conn.cursor()

        # ====================================================
        # PROCESSED FILES
        # ====================================================

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_files (
                file_name TEXT NOT NULL,
                file_etag TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        cursor.execute(
            """
            ALTER TABLE processed_files
            ADD COLUMN IF NOT EXISTS bucket_name TEXT;
            """
        )

        # ====================================================
        # MIGRATE OLD RECORDS
        # ====================================================

        if LEGACY_DEFAULT_BUCKET:

            cursor.execute(
                """
                UPDATE processed_files
                SET bucket_name = %s
                WHERE bucket_name IS NULL
                """,
                (
                    LEGACY_DEFAULT_BUCKET,
                )
            )

        cursor.execute(
            """
            UPDATE processed_files
            SET bucket_name = 'unknown'
            WHERE bucket_name IS NULL
            """
        )

        cursor.execute(
            """
            ALTER TABLE processed_files
            ALTER COLUMN bucket_name SET NOT NULL;
            """
        )

        # ====================================================
        # OLD PRIMARY KEY
        # ====================================================

        cursor.execute(
            """
            ALTER TABLE processed_files
            DROP CONSTRAINT IF EXISTS processed_files_pkey;
            """
        )

        # ====================================================
        # REMOVE DUPLICATES
        # ====================================================

        cursor.execute(
            """
            DELETE FROM processed_files a
            USING processed_files b
            WHERE a.ctid < b.ctid
              AND a.bucket_name = b.bucket_name
              AND a.file_name = b.file_name;
            """
        )

        # ====================================================
        # NEW PRIMARY KEY
        # ====================================================

        cursor.execute(
            """
            ALTER TABLE processed_files
            ADD CONSTRAINT processed_files_pkey
            PRIMARY KEY (
                bucket_name,
                file_name
            );
            """
        )

        # ====================================================
        # CDC EVENTS
        # ====================================================

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cdc_events (

                id BIGSERIAL PRIMARY KEY,

                event_id BIGINT,
                event_type TEXT,
                event_timestamp TIMESTAMP,

                topic TEXT,
                partition_number INTEGER,
                kafka_offset BIGINT,

                database_name TEXT,
                schema_name TEXT,
                table_name TEXT,

                record_id TEXT,

                before_data JSONB,
                after_data JSONB,

                ddl_statement TEXT,

                snapshot BOOLEAN,
                source_lsn BIGINT,
                source_txid BIGINT,

                source_bucket TEXT,
                source_file TEXT,
                source_line_number INTEGER,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        # ====================================================
        # MIGRATION COLUMNS
        # ====================================================

        alter_statements = [

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS event_id BIGINT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS event_type TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS event_timestamp TIMESTAMP;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS topic TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS partition_number INTEGER;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS database_name TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS schema_name TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS table_name TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS record_id TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS before_data JSONB;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS after_data JSONB;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS ddl_statement TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS snapshot BOOLEAN;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS source_lsn BIGINT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS source_txid BIGINT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS source_bucket TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS source_file TEXT;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS source_line_number INTEGER;
            """,

            """
            ALTER TABLE cdc_events
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP
            DEFAULT CURRENT_TIMESTAMP;
            """
        ]

        for statement in alter_statements:

            cursor.execute(
                statement
            )

        # ====================================================
        # MIGRATE OLD CDC RECORDS
        # ====================================================

        if LEGACY_DEFAULT_BUCKET:

            cursor.execute(
                """
                UPDATE cdc_events
                SET source_bucket = %s
                WHERE source_bucket IS NULL
                """,
                (
                    LEGACY_DEFAULT_BUCKET,
                )
            )

        # ====================================================
        # INDEXES
        # ====================================================

        indexes = [

            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_timestamp
            ON cdc_events(event_timestamp);
            """,

            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_type
            ON cdc_events(event_type);
            """,

            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_table
            ON cdc_events(table_name);
            """,

            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_record
            ON cdc_events(record_id);
            """,

            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_source_file
            ON cdc_events(source_file);
            """,

            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_source_bucket
            ON cdc_events(source_bucket);
            """,

            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_source_bucket_file
            ON cdc_events(
                source_bucket,
                source_file
            );
            """,

            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_topic_partition_offset
            ON cdc_events(
                topic,
                partition_number,
                kafka_offset
            );
            """
        ]

        for statement in indexes:

            cursor.execute(
                statement
            )

        # ====================================================
        # OLD UNIQUE INDEXES
        # ====================================================

        cursor.execute(
            """
            DROP INDEX IF EXISTS
            uq_cdc_event_source_file_event_id;
            """
        )

        cursor.execute(
            """
            DROP INDEX IF EXISTS
            uq_cdc_event_file_topic_partition_offset;
            """
        )

        cursor.execute(
            """
            DROP INDEX IF EXISTS
            uq_cdc_event_file_event_id_fallback;
            """
        )

        # ====================================================
        # KAFKA UNIQUE INDEX
        # ====================================================

        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
            uq_cdc_event_bucket_file_topic_partition_offset
            ON cdc_events(
                source_bucket,
                source_file,
                topic,
                partition_number,
                kafka_offset
            )
            WHERE
                topic IS NOT NULL
                AND partition_number IS NOT NULL
                AND kafka_offset IS NOT NULL;
            """
        )

        # ====================================================
        # NON-KAFKA UNIQUE INDEX
        # ====================================================

        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
            uq_cdc_event_bucket_file_event_id_fallback
            ON cdc_events(
                source_bucket,
                source_file,
                event_id
            )
            WHERE
                (
                    topic IS NULL
                    OR partition_number IS NULL
                    OR kafka_offset IS NULL
                )
                AND event_id IS NOT NULL;
            """
        )

        conn.commit()

        cursor.close()

        print(
            "Database initialized successfully.",
            flush=True
        )

    except Exception:

        conn.rollback()

        raise

    finally:

        conn.close()


# ============================================================
# GET PROCESSED FILE INFO
# ============================================================

def get_processed_file_info(
    bucket_name,
    file_name
):

    conn = get_db_connection()

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                file_etag,
                processed_at
            FROM processed_files
            WHERE bucket_name = %s
              AND file_name = %s
            """,
            (
                bucket_name,
                file_name,
            )
        )

        result = cursor.fetchone()

        cursor.close()

        if result:

            return {
                "etag": result[0],
                "processed_at": result[1]
            }

        return None

    finally:

        conn.close()


# ============================================================
# GET PROCESSED ETAG
# ============================================================

def get_processed_etag(
    bucket_name,
    file_name
):

    info = get_processed_file_info(
        bucket_name,
        file_name
    )

    if info is None:

        return None

    return info["etag"]


# ============================================================
# COUNT EVENTS FOR FILE
# ============================================================

def count_events_for_file(
    bucket_name,
    file_name
):

    conn = get_db_connection()

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM cdc_events
            WHERE source_bucket = %s
              AND source_file = %s
            """,
            (
                bucket_name,
                file_name,
            )
        )

        result = cursor.fetchone()

        cursor.close()

        return int(
            result[0]
        ) if result else 0

    finally:

        conn.close()


# ============================================================
# SHOULD PROCESS FILE
# ============================================================

def should_process_file(
    bucket_name,
    file_name,
    etag
):

    print(
        "",
        flush=True
    )

    print(
        f"Checking: {bucket_name}/{file_name}",
        flush=True
    )

    print(
        f"ETag: {etag}",
        flush=True
    )

    if FORCE_REPROCESS:

        print(
            "PROCESS: FORCE_REPROCESS is enabled.",
            flush=True
        )

        return True

    info = get_processed_file_info(
        bucket_name,
        file_name
    )

    if info is None:

        print(
            "PROCESS: File has never been processed.",
            flush=True
        )

        return True

    previous_etag = info.get(
        "etag"
    )

    if previous_etag != etag:

        print(
            "PROCESS: ETag changed.",
            flush=True
        )

        print(
            f"  Previous ETag: {previous_etag}",
            flush=True
        )

        print(
            f"  Current ETag : {etag}",
            flush=True
        )

        return True

    event_count = count_events_for_file(
        bucket_name,
        file_name
    )

    print(
        f"Existing CDC events: {event_count}",
        flush=True
    )

    if event_count == 0:

        print(
            "PROCESS: File was marked processed "
            "but has zero CDC events.",
            flush=True
        )

        return True

    print(
        f"SKIP: {bucket_name}/{file_name}",
        flush=True
    )

    return False


# ============================================================
# TIMESTAMP CONVERSION
# ============================================================

def convert_timestamp_ms(
    value
):

    if value is None:

        return None

    try:

        value = int(
            value
        )

        timestamp = datetime.fromtimestamp(
            value / 1000,
            tz=timezone.utc
        )

        return timestamp.replace(
            tzinfo=None
        )

    except Exception:

        return None


def convert_timestamp_us(
    value
):

    if value is None:

        return None

    try:

        value = int(
            value
        )

        timestamp = datetime.fromtimestamp(
            value / 1_000_000,
            tz=timezone.utc
        )

        return timestamp.replace(
            tzinfo=None
        )

    except Exception:

        return None


# ============================================================
# CSV TIMESTAMP
# ============================================================

def parse_csv_timestamp(
    value
):

    if value is None:

        return None

    value = str(
        value
    ).strip()

    if not value:

        return None

    formats = [
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]

    for fmt in formats:

        try:

            return datetime.strptime(
                value,
                fmt
            )

        except ValueError:

            pass

    try:

        iso_value = value

        if iso_value.endswith(
            "Z"
        ):

            iso_value = (
                iso_value[:-1]
                + "+00:00"
            )

        dt = datetime.fromisoformat(
            iso_value
        )

        if dt.tzinfo:

            dt = dt.astimezone(
                timezone.utc
            )

            dt = dt.replace(
                tzinfo=None
            )

        return dt

    except Exception:

        pass

    return convert_timestamp_ms(
        value
    )


# ============================================================
# SAFE JSON VALUE
# ============================================================

def safe_json_value(
    value
):

    if value is None:

        return None

    if isinstance(
        value,
        (
            dict,
            list,
            int,
            float,
            bool
        )
    ):

        return value

    if isinstance(
        value,
        str
    ):

        value = value.strip()

        if not value:

            return None

        if value.lower() == "null":

            return None

        try:

            return json.loads(
                value
            )

        except Exception:

            return value

    return value


# ============================================================
# DEBEZIUM OPERATION
# ============================================================

def convert_debezium_operation(
    op
):

    if op is None:

        return "UNKNOWN"

    mapping = {

        "c": "INSERT",
        "u": "UPDATE",
        "d": "DELETE",
        "r": "READ",
        "t": "TRUNCATE",
        "m": "MESSAGE",

        "insert": "INSERT",
        "create": "INSERT",

        "update": "UPDATE",

        "delete": "DELETE",

        "read": "READ",
        "snapshot": "READ",

        "truncate": "TRUNCATE",

        "message": "MESSAGE",

        "ddl": "DDL",
    }

    normalized = str(
        op
    ).strip().lower()

    return mapping.get(
        normalized,
        "UNKNOWN"
    )


# ============================================================
# RECORD ID
# ============================================================

def find_record_id(
    after_data,
    before_data,
    table_name
):

    record = None

    if isinstance(
        after_data,
        dict
    ):

        record = after_data

    elif isinstance(
        before_data,
        dict
    ):

        record = before_data

    if not record:

        return None

    candidates = [

        "id",
        "emp_id",
        "employee_id",
        "customer_id",
        "order_id",
        "user_id",
        "product_id",
        "account_id",
        "record_id",
        f"{table_name}_id",
    ]

    for key in candidates:

        if key in record:

            value = record[key]

            if value is not None:

                return str(
                    value
                )

    try:

        first_key = next(
            iter(record)
        )

        value = record[
            first_key
        ]

        if value is not None:

            return str(
                value
            )

    except Exception:

        pass

    return None


# ============================================================
# PARSE TOPIC
# ============================================================

def parse_topic(
    topic,
    database_name=None,
    table_name=None
):

    schema_name = None

    topic = (
        str(topic).strip()
        if topic
        else None
    )

    if topic:

        parts = topic.split(".")

        if len(parts) >= 3:

            schema_name = parts[-2]

            topic_table = parts[-1]

            if not table_name:

                table_name = topic_table

        elif len(parts) == 2:

            schema_name = parts[0]

            if not table_name:

                table_name = parts[1]

    return (
        schema_name,
        table_name
    )


# ============================================================
# GET DEBEZIUM PAYLOAD
# ============================================================

def get_payload(
    event
):

    if not isinstance(
        event,
        dict
    ):

        return None

    value = event.get(
        "value"
    )

    if isinstance(
        value,
        dict
    ):

        payload = value.get(
            "payload"
        )

        if isinstance(
            payload,
            dict
        ):

            if (
                "op" in payload
                or "before" in payload
                or "after" in payload
            ):

                return payload

        if (
            "op" in value
            or "before" in value
            or "after" in value
        ):

            return value

    payload = event.get(
        "payload"
    )

    if isinstance(
        payload,
        dict
    ):

        if (
            "op" in payload
            or "before" in payload
            or "after" in payload
        ):

            return payload

    if (
        "op" in event
        or "before" in event
        or "after" in event
    ):

        return event

    return None


# ============================================================
# EVENT METADATA
# ============================================================

def get_event_metadata(
    event
):

    topic = event.get(
        "topic"
    )

    partition = event.get(
        "partition"
    )

    kafka_offset = event.get(
        "offset"
    )

    try:

        if partition is not None:

            partition = int(
                partition
            )

    except Exception:

        partition = None

    try:

        if kafka_offset is not None:

            kafka_offset = int(
                kafka_offset
            )

    except Exception:

        kafka_offset = None

    timestamp_ms = event.get(
        "tsMs"
    )

    if timestamp_ms is None:

        timestamp_ms = event.get(
            "timestamp"
        )

    return (
        topic,
        partition,
        kafka_offset,
        timestamp_ms
    )


# ============================================================
# PARSE ONE DEBEZIUM EVENT
# ============================================================

def parse_debezium_event(
    event,
    file_name,
    line_number
):

    if not isinstance(
        event,
        dict
    ):

        return None

    payload = get_payload(
        event
    )

    if not isinstance(
        payload,
        dict
    ):

        return None

    before_data = payload.get(
        "before"
    )

    after_data = payload.get(
        "after"
    )

    op = payload.get(
        "op"
    )

    event_type = convert_debezium_operation(
        op
    )

    if event_type == "UNKNOWN":

        return None

    (
        topic,
        partition,
        kafka_offset,
        envelope_timestamp_ms
    ) = get_event_metadata(
        event
    )

    source = payload.get(
        "source"
    )

    if not isinstance(
        source,
        dict
    ):

        source = {}

    database_name = source.get(
        "db"
    )

    schema_name = source.get(
        "schema"
    )

    table_name = source.get(
        "table"
    )

    if not table_name:

        table_name = "unknown"

    if not topic:

        connector_name = source.get(
            "name"
        )

        if connector_name:

            if schema_name:

                topic = (
                    f"{connector_name}."
                    f"{schema_name}."
                    f"{table_name}"
                )

            else:

                topic = (
                    f"{connector_name}."
                    f"{table_name}"
                )

    topic_schema, topic_table = parse_topic(
        topic,
        database_name,
        table_name
    )

    if not schema_name:

        schema_name = topic_schema

    if (
        not table_name
        or table_name == "unknown"
    ):

        if topic_table:

            table_name = topic_table

    timestamp_ms = payload.get(
        "ts_ms"
    )

    if timestamp_ms is None:

        timestamp_ms = payload.get(
            "tsMs"
        )

    if timestamp_ms is None:

        timestamp_ms = source.get(
            "ts_ms"
        )

    if timestamp_ms is None:

        timestamp_ms = envelope_timestamp_ms

    event_timestamp = convert_timestamp_ms(
        timestamp_ms
    )

    snapshot_value = source.get(
        "snapshot"
    )

    snapshot = None

    if snapshot_value is not None:

        snapshot = (
            str(
                snapshot_value
            ).strip().lower()
            in (
                "true",
                "1",
                "yes",
                "y",
                "last",
                "incremental"
            )
        )

    source_lsn = source.get(
        "lsn"
    )

    try:

        if source_lsn is not None:

            source_lsn = int(
                source_lsn
            )

    except Exception:

        source_lsn = None

    source_txid = source.get(
        "txId"
    )

    try:

        if source_txid is not None:

            source_txid = int(
                source_txid
            )

    except Exception:

        source_txid = None

    record_id = find_record_id(
        after_data,
        before_data,
        table_name
    )

    return {

        "event_id":
            line_number,

        "event_type":
            event_type,

        "event_timestamp":
            event_timestamp,

        "topic":
            topic,

        "partition_number":
            partition,

        "kafka_offset":
            kafka_offset,

        "database_name":
            database_name,

        "schema_name":
            schema_name,

        "table_name":
            table_name,

        "record_id":
            record_id,

        "before_data":
            safe_json_value(
                before_data
            ),

        "after_data":
            safe_json_value(
                after_data
            ),

        "ddl_statement":
            None,

        "snapshot":
            snapshot,

        "source_lsn":
            source_lsn,

        "source_txid":
            source_txid,

        "source_line_number":
            line_number,

        "source_file":
            file_name,
    }


# ============================================================
# PARSE JSONL / NDJSON
# ============================================================

def parse_jsonl(
    data,
    file_name
):

    print(
        f"Reading JSONL/NDJSON: {file_name}",
        flush=True
    )

    try:

        text = data.decode(
            "utf-8-sig"
        )

    except Exception as e:

        print(
            f"ERROR decoding JSONL {file_name}: {e}",
            flush=True
        )

        return []

    if not text.strip():

        return []

    events = []

    total_lines = 0
    invalid_lines = 0
    ignored_lines = 0
    operation_counts = {}

    for line_number, raw_line in enumerate(
        text.splitlines(),
        start=1
    ):

        total_lines += 1

        line = raw_line.strip()

        if not line:

            continue

        try:

            event = json.loads(
                line
            )

        except json.JSONDecodeError as e:

            invalid_lines += 1

            if invalid_lines <= 5:

                print(
                    f"WARNING: Invalid JSON "
                    f"line {line_number}: {e}",
                    flush=True
                )

            continue

        if not isinstance(
            event,
            dict
        ):

            ignored_lines += 1

            continue

        parsed_event = parse_debezium_event(
            event,
            file_name,
            line_number
        )

        if parsed_event is None:

            ignored_lines += 1

            continue

        events.append(
            parsed_event
        )

        operation = parsed_event[
            "event_type"
        ]

        operation_counts[
            operation
        ] = (
            operation_counts.get(
                operation,
                0
            )
            + 1
        )

    print(
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        f"JSONL parsing complete: {file_name}",
        flush=True
    )

    print(
        f"Total lines        : {total_lines}",
        flush=True
    )

    print(
        f"CDC events parsed  : {len(events)}",
        flush=True
    )

    print(
        f"Invalid JSON lines : {invalid_lines}",
        flush=True
    )

    print(
        f"Ignored lines      : {ignored_lines}",
        flush=True
    )

    print(
        f"Operations         : {operation_counts}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    return events


# ============================================================
# PARSE NORMAL JSON
# ============================================================

def parse_cdc_json(
    data,
    file_name
):

    print(
        f"Reading JSON: {file_name}",
        flush=True
    )

    try:

        text = data.decode(
            "utf-8-sig"
        ).strip()

    except Exception as e:

        print(
            f"ERROR decoding JSON {file_name}: {e}",
            flush=True
        )

        return []

    if not text:

        return []

    try:

        parsed = json.loads(
            text
        )

    except json.JSONDecodeError:

        print(
            "Complete JSON decode failed.",
            flush=True
        )

        print(
            "Falling back to JSONL/NDJSON.",
            flush=True
        )

        return parse_jsonl(
            data,
            file_name
        )

    if isinstance(
        parsed,
        list
    ):

        json_events = parsed

    elif isinstance(
        parsed,
        dict
    ):

        if get_payload(
            parsed
        ) is not None:

            json_events = [
                parsed
            ]

        elif isinstance(
            parsed.get("events"),
            list
        ):

            json_events = parsed[
                "events"
            ]

        elif isinstance(
            parsed.get("records"),
            list
        ):

            json_events = parsed[
                "records"
            ]

        else:

            json_events = []

    else:

        json_events = []

    events = []
    ignored = 0

    for index, event in enumerate(
        json_events,
        start=1
    ):

        parsed_event = parse_debezium_event(
            event,
            file_name,
            index
        )

        if parsed_event:

            events.append(
                parsed_event
            )

        else:

            ignored += 1

    print(
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        f"JSON parsing complete: {file_name}",
        flush=True
    )

    print(
        f"Objects found      : {len(json_events)}",
        flush=True
    )

    print(
        f"CDC events parsed  : {len(events)}",
        flush=True
    )

    print(
        f"Ignored objects    : {ignored}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    return events


# ============================================================
# SIMPLE CDC CSV
# ============================================================

def parse_simple_cdc_csv(
    reader,
    file_name
):

    print(
        "Detected CSV format: SIMPLE CDC",
        flush=True
    )

    rows = []

    operation_mapping = {

        "INSERT": "INSERT",
        "CREATE": "INSERT",
        "C": "INSERT",

        "UPDATE": "UPDATE",
        "U": "UPDATE",

        "DELETE": "DELETE",
        "D": "DELETE",

        "READ": "READ",
        "R": "READ",

        "SNAPSHOT": "READ",

        "TRUNCATE": "TRUNCATE",
        "T": "TRUNCATE",

        "MESSAGE": "MESSAGE",
        "M": "MESSAGE",

        "DDL": "DDL",
    }

    for line_number, csv_row in enumerate(
        reader,
        start=2
    ):

        try:

            if not csv_row:

                continue

            if all(
                value is None
                or not str(value).strip()
                for value in csv_row.values()
            ):

                continue

            raw_event_id = (
                csv_row.get(
                    "event_id"
                )
                or ""
            ).strip()

            try:

                event_id = int(
                    raw_event_id
                )

            except Exception:

                event_id = line_number

            raw_event_type = (
                csv_row.get(
                    "event_type"
                )
                or ""
            ).strip().upper()

            event_type = operation_mapping.get(
                raw_event_type,
                "UNKNOWN"
            )

            if event_type == "UNKNOWN":

                continue

            event_timestamp = parse_csv_timestamp(
                csv_row.get(
                    "event_timestamp"
                )
            )

            table_name = (
                csv_row.get(
                    "table_name"
                )
                or ""
            ).strip()

            if not table_name:

                table_name = "unknown"

            record_id = (
                csv_row.get(
                    "record_id"
                )
                or ""
            ).strip()

            if not record_id:

                record_id = None

            before_data = safe_json_value(
                csv_row.get(
                    "before_data"
                )
            )

            after_data = safe_json_value(
                csv_row.get(
                    "after_data"
                )
            )

            ddl_statement = (
                csv_row.get(
                    "ddl_statement"
                )
                or ""
            ).strip()

            if not ddl_statement:

                ddl_statement = None

            rows.append({

                "event_id":
                    event_id,

                "event_type":
                    event_type,

                "event_timestamp":
                    event_timestamp,

                "topic":
                    None,

                "partition_number":
                    None,

                "kafka_offset":
                    None,

                "database_name":
                    None,

                "schema_name":
                    None,

                "table_name":
                    table_name,

                "record_id":
                    record_id,

                "before_data":
                    before_data,

                "after_data":
                    after_data,

                "ddl_statement":
                    ddl_statement,

                "snapshot":
                    None,

                "source_lsn":
                    None,

                "source_txid":
                    None,

                "source_line_number":
                    line_number,

                "source_file":
                    file_name,
            })

        except Exception as e:

            print(
                f"ERROR parsing simple CDC CSV "
                f"{file_name}, line {line_number}: {e}",
                flush=True
            )

    print(
        f"Simple CDC CSV events: {len(rows)}",
        flush=True
    )

    return rows


# ============================================================
# KAFKA CDC CSV
# ============================================================

def parse_kafka_cdc_csv(
    reader,
    file_name
):

    print(
        "Detected CSV format: KAFKA CDC",
        flush=True
    )

    rows = []

    operation_mapping = {

        "INSERT": "INSERT",
        "CREATE": "INSERT",
        "C": "INSERT",

        "UPDATE": "UPDATE",
        "U": "UPDATE",

        "DELETE": "DELETE",
        "D": "DELETE",

        "READ": "READ",
        "R": "READ",

        "SNAPSHOT": "READ",

        "TRUNCATE": "TRUNCATE",
        "T": "TRUNCATE",

        "MESSAGE": "MESSAGE",
        "M": "MESSAGE",

        "DDL": "DDL",
    }

    for line_number, csv_row in enumerate(
        reader,
        start=2
    ):

        try:

            if not csv_row:

                continue

            if all(
                value is None
                or not str(value).strip()
                for value in csv_row.values()
            ):

                continue

            event_timestamp = parse_csv_timestamp(
                csv_row.get(
                    "Timestamp (UTC)"
                )
            )

            topic = (
                csv_row.get(
                    "Topic"
                )
                or ""
            ).strip()

            topic = topic or None

            partition = None

            partition_raw = (
                csv_row.get(
                    "Partition"
                )
                or ""
            ).strip()

            if partition_raw:

                try:

                    partition = int(
                        partition_raw
                    )

                except ValueError:

                    pass

            kafka_offset = None

            offset_raw = (
                csv_row.get(
                    "Offset"
                )
                or ""
            ).strip()

            if offset_raw:

                try:

                    kafka_offset = int(
                        offset_raw
                    )

                except ValueError:

                    pass

            operation = (
                csv_row.get(
                    "Operation"
                )
                or ""
            ).strip().upper()

            event_type = operation_mapping.get(
                operation
            )

            if event_type is None:

                continue

            database_name = (
                csv_row.get(
                    "Database"
                )
                or ""
            ).strip()

            database_name = (
                database_name
                or None
            )

            table_name = (
                csv_row.get(
                    "Table"
                )
                or ""
            ).strip()

            table_name = (
                table_name
                or None
            )

            key_data = safe_json_value(
                csv_row.get(
                    "Key"
                )
            )

            before_data = safe_json_value(
                csv_row.get(
                    "Before"
                )
            )

            after_data = safe_json_value(
                csv_row.get(
                    "After"
                )
            )

            schema_name, topic_table = parse_topic(
                topic,
                database_name,
                table_name
            )

            if not table_name:

                table_name = topic_table

            if not table_name:

                table_name = "unknown"

            record_id = None

            if isinstance(
                key_data,
                dict
            ):

                record_id = find_record_id(
                    key_data,
                    None,
                    table_name
                )

            if record_id is None:

                record_id = find_record_id(
                    after_data,
                    before_data,
                    table_name
                )

            rows.append({

                "event_id":
                    line_number,

                "event_type":
                    event_type,

                "event_timestamp":
                    event_timestamp,

                "topic":
                    topic,

                "partition_number":
                    partition,

                "kafka_offset":
                    kafka_offset,

                "database_name":
                    database_name,

                "schema_name":
                    schema_name,

                "table_name":
                    table_name,

                "record_id":
                    record_id,

                "before_data":
                    before_data,

                "after_data":
                    after_data,

                "ddl_statement":
                    None,

                "snapshot":
                    None,

                "source_lsn":
                    None,

                "source_txid":
                    None,

                "source_line_number":
                    line_number,

                "source_file":
                    file_name,
            })

        except Exception as e:

            print(
                f"ERROR parsing Kafka CSV "
                f"{file_name}, line {line_number}: {e}",
                flush=True
            )

    print(
        f"Kafka CDC CSV events: {len(rows)}",
        flush=True
    )

    return rows


# ============================================================
# PARSE CSV
# ============================================================

def parse_cdc_csv(
    data,
    file_name
):

    print(
        f"Reading CSV: {file_name}",
        flush=True
    )

    try:

        text = data.decode(
            "utf-8-sig"
        )

    except Exception as e:

        print(
            f"ERROR decoding CSV: {e}",
            flush=True
        )

        return []

    if not text.strip():

        return []

    reader = csv.DictReader(
        StringIO(text)
    )

    if not reader.fieldnames:

        print(
            "ERROR: CSV has no header.",
            flush=True
        )

        return []

    reader.fieldnames = [

        header.strip()
        if header is not None
        else None

        for header in reader.fieldnames
    ]

    headers = set(
        reader.fieldnames
    )

    simple_columns = {

        "event_id",
        "event_type",
        "event_timestamp",
        "table_name",
        "record_id",
        "before_data",
        "after_data",
        "ddl_statement",
    }

    if simple_columns.issubset(
        headers
    ):

        return parse_simple_cdc_csv(
            reader,
            file_name
        )

    kafka_columns = {

        "Timestamp (UTC)",
        "Topic",
        "Partition",
        "Offset",
        "Operation",
        "Database",
        "Table",
        "Key",
        "Before",
        "After",
    }

    if kafka_columns.issubset(
        headers
    ):

        return parse_kafka_cdc_csv(
            reader,
            file_name
        )

    print(
        "ERROR: Unsupported CSV format.",
        flush=True
    )

    print(
        f"Headers: {reader.fieldnames}",
        flush=True
    )

    return []


# ============================================================
# DELETE EVENTS FOR FILE
# ============================================================

def delete_file_events(
    conn,
    bucket_name,
    file_name
):

    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM cdc_events
        WHERE source_bucket = %s
          AND source_file = %s
        """,
        (
            bucket_name,
            file_name,
        )
    )

    count = cursor.rowcount

    cursor.close()

    return count


# ============================================================
# INSERT EVENTS
# ============================================================

def insert_events(
    conn,
    rows,
    bucket_name,
    file_name,
    etag
):

    cursor = conn.cursor()

    inserted_count = 0
    skipped_count = 0

    for row in rows:

        before_data = row.get(
            "before_data"
        )

        after_data = row.get(
            "after_data"
        )

        cursor.execute(
            """
            INSERT INTO cdc_events (

                event_id,
                event_type,
                event_timestamp,

                topic,
                partition_number,
                kafka_offset,

                database_name,
                schema_name,
                table_name,

                record_id,

                before_data,
                after_data,

                ddl_statement,

                snapshot,
                source_lsn,
                source_txid,

                source_bucket,
                source_file,
                source_line_number

            )
            VALUES (

                %s,
                %s,
                %s,

                %s,
                %s,
                %s,

                %s,
                %s,
                %s,

                %s,

                %s,
                %s,

                %s,

                %s,
                %s,
                %s,

                %s,
                %s,
                %s
            )

            ON CONFLICT DO NOTHING
            """,
            (

                row.get(
                    "event_id"
                ),

                row.get(
                    "event_type"
                ),

                row.get(
                    "event_timestamp"
                ),

                row.get(
                    "topic"
                ),

                row.get(
                    "partition_number"
                ),

                row.get(
                    "kafka_offset"
                ),

                row.get(
                    "database_name"
                ),

                row.get(
                    "schema_name"
                ),

                row.get(
                    "table_name"
                ),

                row.get(
                    "record_id"
                ),

                (
                    Json(before_data)
                    if before_data is not None
                    else None
                ),

                (
                    Json(after_data)
                    if after_data is not None
                    else None
                ),

                row.get(
                    "ddl_statement"
                ),

                row.get(
                    "snapshot"
                ),

                row.get(
                    "source_lsn"
                ),

                row.get(
                    "source_txid"
                ),

                bucket_name,

                file_name,

                row.get(
                    "source_line_number"
                ),
            )
        )

        if cursor.rowcount == 1:

            inserted_count += 1

        else:

            skipped_count += 1

    # ========================================================
    # MARK FILE PROCESSED
    # ========================================================

    cursor.execute(
        """
        INSERT INTO processed_files (
            bucket_name,
            file_name,
            file_etag,
            processed_at
        )
        VALUES (
            %s,
            %s,
            %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (
            bucket_name,
            file_name
        )
        DO UPDATE SET

            file_etag =
                EXCLUDED.file_etag,

            processed_at =
                CURRENT_TIMESTAMP
        """,
        (
            bucket_name,
            file_name,
            etag,
        )
    )

    cursor.close()

    return (
        inserted_count,
        skipped_count
    )


# ============================================================
# PROCESS ONE FILE
# ============================================================

def process_file(
    bucket_name,
    file_name,
    etag
):

    print(
        "",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    print(
        f"PROCESSING: {bucket_name}/{file_name}",
        flush=True
    )

    print(
        f"ETag: {etag}",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    try:

        response = s3.get_object(
            Bucket=bucket_name,
            Key=file_name
        )

        data = response[
            "Body"
        ].read()

    except Exception as e:

        print(
            f"ERROR downloading "
            f"{bucket_name}/{file_name}: {e}",
            flush=True
        )

        return False

    print(
        f"Downloaded {len(data)} bytes.",
        flush=True
    )

    lower_name = file_name.lower()

    if lower_name.endswith(
        ".csv"
    ):

        rows = parse_cdc_csv(
            data,
            file_name
        )

    elif (
        lower_name.endswith(".jsonl")
        or lower_name.endswith(".ndjson")
    ):

        rows = parse_jsonl(
            data,
            file_name
        )

    elif lower_name.endswith(
        ".json"
    ):

        rows = parse_cdc_json(
            data,
            file_name
        )

    else:

        print(
            f"Unsupported file: {file_name}",
            flush=True
        )

        return False

    print(
        f"Parsed {len(rows)} CDC events.",
        flush=True
    )

    if not rows:

        print(
            "",
            flush=True
        )

        print(
            "WARNING: ZERO CDC EVENTS PARSED.",
            flush=True
        )

        print(
            "File will NOT be marked processed.",
            flush=True
        )

        return False

    conn = get_db_connection()

    try:

        previous_etag = get_processed_etag(
            bucket_name,
            file_name
        )

        existing_event_count = count_events_for_file(
            bucket_name,
            file_name
        )

        file_changed = (
            previous_etag is not None
            and previous_etag != etag
        )

        repair_empty_file = (
            previous_etag is not None
            and previous_etag == etag
            and existing_event_count == 0
        )

        if (
            file_changed
            or FORCE_REPROCESS
            or repair_empty_file
        ):

            deleted_count = delete_file_events(
                conn,
                bucket_name,
                file_name
            )

            print(
                f"Deleted old CDC events: {deleted_count}",
                flush=True
            )

        (
            inserted_count,
            skipped_count
        ) = insert_events(
            conn,
            rows,
            bucket_name,
            file_name,
            etag
        )

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM cdc_events
            WHERE source_bucket = %s
              AND source_file = %s
            """,
            (
                bucket_name,
                file_name,
            )
        )

        final_count = int(
            cursor.fetchone()[0]
        )

        cursor.close()

        if final_count == 0:

            raise RuntimeError(
                "File processing produced zero database events."
            )

        conn.commit()

        print(
            "",
            flush=True
        )

        print(
            "SUCCESS",
            flush=True
        )

        print(
            f"Bucket          : {bucket_name}",
            flush=True
        )

        print(
            f"File            : {file_name}",
            flush=True
        )

        print(
            f"Events parsed   : {len(rows)}",
            flush=True
        )

        print(
            f"Events inserted : {inserted_count}",
            flush=True
        )

        print(
            f"Duplicates      : {skipped_count}",
            flush=True
        )

        print(
            f"Events in DB    : {final_count}",
            flush=True
        )

        return True

    except Exception as e:

        conn.rollback()

        print(
            "",
            flush=True
        )

        print(
            f"ERROR processing "
            f"{bucket_name}/{file_name}: {e}",
            flush=True
        )

        return False

    finally:

        conn.close()


# ============================================================
# GET MINIO BUCKETS
# ============================================================

def get_minio_buckets():

    configured_buckets = get_configured_buckets()

    if configured_buckets:

        print(
            "",
            flush=True
        )

        print(
            "Using configured buckets:",
            flush=True
        )

        for bucket in configured_buckets:

            print(
                f"  - {bucket}",
                flush=True
            )

        return configured_buckets

    print(
        "",
        flush=True
    )

    print(
        "MINIO_BUCKETS is empty.",
        flush=True
    )

    print(
        "Discovering all accessible MinIO buckets...",
        flush=True
    )

    response = s3.list_buckets()

    buckets = []

    for bucket in response.get(
        "Buckets",
        []
    ):

        bucket_name = bucket.get(
            "Name"
        )

        if bucket_name:

            buckets.append(
                bucket_name
            )

    buckets.sort()

    print(
        "",
        flush=True
    )

    print(
        "Accessible MinIO buckets:",
        flush=True
    )

    if not buckets:

        print(
            "  NO BUCKETS FOUND",
            flush=True
        )

    else:

        for bucket in buckets:

            print(
                f"  - {bucket}",
                flush=True
            )

    return buckets


# ============================================================
# CHECK BUCKET ACCESS
# ============================================================

def check_bucket_access(
    bucket_name
):

    try:

        s3.head_bucket(
            Bucket=bucket_name
        )

        return True

    except Exception as e:

        print(
            f"WARNING: Cannot access bucket "
            f"'{bucket_name}': {e}",
            flush=True
        )

        return False


# ============================================================
# SCAN ONE BUCKET
# ============================================================

def scan_bucket(
    bucket_name
):

    continuation_token = None

    total_objects = 0
    supported_files = 0
    processed_count = 0
    skipped_count = 0
    failed_count = 0

    print(
        "",
        flush=True
    )

    print(
        "############################################################",
        flush=True
    )

    print(
        f"BUCKET SCAN: {bucket_name}",
        flush=True
    )

    print(
        f"Prefix: {MINIO_PREFIX or '(entire bucket)'}",
        flush=True
    )

    print(
        "############################################################",
        flush=True
    )

    if not check_bucket_access(
        bucket_name
    ):

        return {
            "objects": 0,
            "supported": 0,
            "processed": 0,
            "skipped": 0,
            "failed": 1,
        }

    while True:

        request = {
            "Bucket":
                bucket_name,
            "Prefix":
                MINIO_PREFIX,
        }

        if continuation_token:

            request[
                "ContinuationToken"
            ] = continuation_token

        try:

            response = s3.list_objects_v2(
                **request
            )

        except Exception as e:

            print(
                f"ERROR listing bucket "
                f"{bucket_name}: {e}",
                flush=True
            )

            failed_count += 1

            break

        objects = response.get(
            "Contents",
            []
        )

        total_objects += len(
            objects
        )

        for obj in objects:

            file_name = obj.get(
                "Key"
            )

            if not file_name:

                continue

            if file_name.endswith(
                "/"
            ):

                continue

            lower_name = file_name.lower()

            if not lower_name.endswith(
                SUPPORTED_EXTENSIONS
            ):

                continue

            supported_files += 1

            etag = (
                obj.get(
                    "ETag",
                    ""
                )
                .replace(
                    '"',
                    ""
                )
            )

            try:

                process_required = should_process_file(
                    bucket_name,
                    file_name,
                    etag
                )

            except Exception as e:

                failed_count += 1

                print(
                    f"ERROR checking "
                    f"{bucket_name}/{file_name}: {e}",
                    flush=True
                )

                continue

            if not process_required:

                skipped_count += 1

                continue

            success = process_file(
                bucket_name,
                file_name,
                etag
            )

            if success:

                processed_count += 1

            else:

                failed_count += 1

        if not response.get(
            "IsTruncated",
            False
        ):

            break

        continuation_token = response.get(
            "NextContinuationToken"
        )

        if not continuation_token:

            break

    print(
        "",
        flush=True
    )

    print(
        "############################################################",
        flush=True
    )

    print(
        f"BUCKET COMPLETE: {bucket_name}",
        flush=True
    )

    print(
        f"Objects          : {total_objects}",
        flush=True
    )

    print(
        f"Supported files  : {supported_files}",
        flush=True
    )

    print(
        f"Processed        : {processed_count}",
        flush=True
    )

    print(
        f"Skipped          : {skipped_count}",
        flush=True
    )

    print(
        f"Failed           : {failed_count}",
        flush=True
    )

    print(
        "############################################################",
        flush=True
    )

    return {
        "objects":
            total_objects,

        "supported":
            supported_files,

        "processed":
            processed_count,

        "skipped":
            skipped_count,

        "failed":
            failed_count,
    }


# ============================================================
# SCAN ALL MINIO
# ============================================================

def scan_minio():

    print(
        "",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    print(
        "MINIO MULTI-BUCKET SCAN START",
        flush=True
    )

    print(
        f"Endpoint: {MINIO_ENDPOINT}",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    try:

        buckets = get_minio_buckets()

    except Exception as e:

        print(
            f"ERROR discovering MinIO buckets: {e}",
            flush=True
        )

        return

    if not buckets:

        print(
            "WARNING: No accessible MinIO buckets.",
            flush=True
        )

        return

    totals = {
        "objects": 0,
        "supported": 0,
        "processed": 0,
        "skipped": 0,
        "failed": 0,
    }

    for bucket_name in buckets:

        try:

            result = scan_bucket(
                bucket_name
            )

            for key in totals:

                totals[key] += result.get(
                    key,
                    0
                )

        except Exception as e:

            totals[
                "failed"
            ] += 1

            print(
                f"ERROR scanning bucket "
                f"'{bucket_name}': {e}",
                flush=True
            )

    print(
        "",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )

    print(
        "MINIO MULTI-BUCKET SCAN COMPLETE",
        flush=True
    )

    print(
        f"Buckets scanned : {len(buckets)}",
        flush=True
    )

    print(
        f"Objects         : {totals['objects']}",
        flush=True
    )

    print(
        f"Supported files : {totals['supported']}",
        flush=True
    )

    print(
        f"Processed       : {totals['processed']}",
        flush=True
    )

    print(
        f"Skipped         : {totals['skipped']}",
        flush=True
    )

    print(
        f"Failed          : {totals['failed']}",
        flush=True
    )

    print(
        "============================================================",
        flush=True
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print_configuration()

    # ========================================================
    # DATABASE RETRY
    # ========================================================

    while True:

        try:

            initialize_database()

            break

        except Exception as e:

            print(
                "",
                flush=True
            )

            print(
                f"Database initialization failed: {e}",
                flush=True
            )

            print(
                "Retrying database in 5 seconds...",
                flush=True
            )

            time.sleep(
                5
            )

    # ========================================================
    # MINIO RETRY
    # ========================================================

    while True:

        if test_minio_connection():

            break

        print(
            "",
            flush=True
        )

        print(
            "MinIO is not reachable/authentication failed.",
            flush=True
        )

        print(
            "Retrying MinIO connection in 10 seconds...",
            flush=True
        )

        time.sleep(
            10
        )

    # ========================================================
    # CONTINUOUS POLLING
    # ========================================================

    while True:

        try:

            scan_minio()

        except Exception as e:

            print(
                "",
                flush=True
            )

            print(
                f"ERROR during MinIO scan: {e}",
                flush=True
            )

        time.sleep(
            POLL_INTERVAL
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
