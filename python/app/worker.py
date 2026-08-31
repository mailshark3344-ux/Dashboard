import os
import time
import csv
import json
from datetime import datetime, timezone
from io import StringIO

import boto3
import psycopg2
from psycopg2.extras import Json


# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://host.docker.internal:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin123"
)

# ------------------------------------------------------------
# BUCKET CONFIGURATION
# ------------------------------------------------------------
#
# Preferred:
#
# MINIO_BUCKETS=customer-a,customer-b,cdc-production
#
# The worker will scan only these buckets.
#
# If MINIO_BUCKETS is empty, the worker will discover all
# buckets accessible by the configured MinIO credentials.
#
# For security, it is recommended to explicitly configure
# MINIO_BUCKETS rather than allowing automatic discovery.
#
# ------------------------------------------------------------

MINIO_BUCKETS_RAW = os.getenv(
    "MINIO_BUCKETS",
    ""
).strip()

# ------------------------------------------------------------
# Backward compatibility
# ------------------------------------------------------------
#
# Older configuration used:
#
# MINIO_BUCKET=myfiles
#
# If MINIO_BUCKETS is not supplied, this value is also accepted.
#
# ------------------------------------------------------------

LEGACY_MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    ""
).strip()

# ------------------------------------------------------------
# Legacy bucket used for database migration
# ------------------------------------------------------------
#
# Existing processed_files / cdc_events rows from the old
# single-bucket implementation need a bucket name.
#
# If you previously used "myfiles", this remains the default.
#
# ------------------------------------------------------------

LEGACY_DEFAULT_BUCKET = os.getenv(
    "LEGACY_DEFAULT_BUCKET",
    LEGACY_MINIO_BUCKET or "myfiles"
).strip()

# ------------------------------------------------------------
# PREFIX
# ------------------------------------------------------------
#
# Example:
#
# MINIO_PREFIX=cdc data/cdc/
#
# Empty means scan the entire bucket.
#
# ------------------------------------------------------------

MINIO_PREFIX = os.getenv(
    "MINIO_PREFIX",
    ""
)

POLL_INTERVAL = int(
    os.getenv(
        "POLL_INTERVAL",
        "10"
    )
)

# ------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "postgres"
)

POSTGRES_PORT = int(
    os.getenv(
        "POSTGRES_PORT",
        "5432"
    )
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "redash"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "redash"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "redashpass"
)


# ============================================================
# FORCE REPROCESS
# ============================================================

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
# DISCOVER BUCKETS
# ============================================================

def get_configured_buckets():
    """
    Return explicitly configured buckets.

    Priority:

    1. MINIO_BUCKETS
    2. MINIO_BUCKET
    3. Empty list

    Empty list means automatic bucket discovery is enabled.
    """

    buckets = []

    if MINIO_BUCKETS_RAW:

        for bucket in MINIO_BUCKETS_RAW.split(","):

            bucket = bucket.strip()

            if bucket and bucket not in buckets:

                buckets.append(bucket)

    elif LEGACY_MINIO_BUCKET:

        buckets.append(
            LEGACY_MINIO_BUCKET
        )

    return buckets


# ============================================================
# MINIO CLIENT
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)


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

        # ====================================================
        # ADD BUCKET COLUMN
        # ====================================================

        cursor.execute(
            """
            ALTER TABLE processed_files
            ADD COLUMN IF NOT EXISTS bucket_name TEXT;
            """
        )

        # ====================================================
        # MIGRATE EXISTING processed_files
        #
        # Old version had:
        #
        # file_name PRIMARY KEY
        #
        # Assign old records to LEGACY_DEFAULT_BUCKET.
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

        # ====================================================
        # REMOVE NULLS
        # ====================================================

        cursor.execute(
            """
            UPDATE processed_files
            SET bucket_name = 'unknown'
            WHERE bucket_name IS NULL
            """
        )

        # ====================================================
        # MAKE bucket_name NOT NULL
        # ====================================================

        cursor.execute(
            """
            ALTER TABLE processed_files
            ALTER COLUMN bucket_name SET NOT NULL;
            """
        )

        # ====================================================
        # DROP OLD PRIMARY KEY
        # ====================================================
        #
        # Older installation normally created:
        #
        # processed_files_pkey
        #
        # We remove it so bucket + file can become the key.
        #
        # ====================================================

        cursor.execute(
            """
            ALTER TABLE processed_files
            DROP CONSTRAINT IF EXISTS processed_files_pkey;
            """
        )

        # ====================================================
        # REMOVE POSSIBLE DUPLICATES
        #
        # This protects migration if duplicate rows somehow
        # exist for the same bucket/file.
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
        # MIGRATE EXISTING CDC EVENTS
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

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_timestamp
            ON cdc_events(event_timestamp);
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_type
            ON cdc_events(event_type);
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_table
            ON cdc_events(table_name);
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_record
            ON cdc_events(record_id);
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_source_file
            ON cdc_events(source_file);
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_source_bucket
            ON cdc_events(source_bucket);
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_source_bucket_file
            ON cdc_events(
                source_bucket,
                source_file
            );
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_cdc_events_topic_partition_offset
            ON cdc_events(
                topic,
                partition_number,
                kafka_offset
            );
            """
        )

        # ====================================================
        # REMOVE OLD UNIQUE INDEXES
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
        # KAFKA EVENT UNIQUE INDEX
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
        # NON-KAFKA FALLBACK UNIQUE INDEX
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
# GET PROCESSED FILE INFORMATION
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

        if result:

            return int(
                result[0]
            )

        return 0

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

    # --------------------------------------------------------
    # FORCE
    # --------------------------------------------------------

    if FORCE_REPROCESS:

        print(
            "PROCESS: FORCE_REPROCESS is enabled.",
            flush=True
        )

        return True

    # --------------------------------------------------------
    # GET PREVIOUS
    # --------------------------------------------------------

    info = get_processed_file_info(
        bucket_name,
        file_name
    )

    # --------------------------------------------------------
    # NEVER PROCESSED
    # --------------------------------------------------------

    if info is None:

        print(
            "PROCESS: File has never been processed.",
            flush=True
        )

        return True

    previous_etag = info.get(
        "etag"
    )

    # --------------------------------------------------------
    # ETAG CHANGED
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # SAME ETAG
    # --------------------------------------------------------

    event_count = count_events_for_file(
        bucket_name,
        file_name
    )

    print(
        f"  Existing CDC events: {event_count}",
        flush=True
    )

    # --------------------------------------------------------
    # REPAIR
    # --------------------------------------------------------

    if event_count == 0:

        print(
            "PROCESS: File is marked processed but "
            "has ZERO CDC events.",
            flush=True
        )

        print(
            "PROCESS: Reprocessing file.",
            flush=True
        )

        return True

    # --------------------------------------------------------
    # NORMAL SKIP
    # --------------------------------------------------------

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


# ============================================================
# MICROSECONDS TIMESTAMP
# ============================================================

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

    # --------------------------------------------------------
    # DD-MM-YYYY HH:MM
    # --------------------------------------------------------

    try:

        return datetime.strptime(
            value,
            "%d-%m-%Y %H:%M"
        )

    except ValueError:

        pass

    # --------------------------------------------------------
    # DD-MM-YYYY HH:MM:SS
    # --------------------------------------------------------

    try:

        return datetime.strptime(
            value,
            "%d-%m-%Y %H:%M:%S"
        )

    except ValueError:

        pass

    # --------------------------------------------------------
    # ISO
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Epoch milliseconds
    # --------------------------------------------------------

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

            value = record[
                key
            ]

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
                or
                "before" in payload
                or
                "after" in payload
            ):

                return payload

        if (
            "op" in value
            or
            "before" in value
            or
            "after" in value
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
            or
            "before" in payload
            or
            "after" in payload
        ):

            return payload

    if (
        "op" in event
        or
        "before" in event
        or
        "after" in event
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

    event_id = line_number

    return {

        "event_id":
            event_id,

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
            f"ERROR decoding JSONL "
            f"{file_name}: {e}",
            flush=True
        )

        return []

    if not text.strip():

        print(
            "JSONL file is empty.",
            flush=True
        )

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

        if total_lines == 1:

            print(
                "",
                flush=True
            )

            print(
                "FIRST JSONL RECORD:",
                flush=True
            )

            print(
                f"  Top-level keys: "
                f"{list(event.keys())}",
                flush=True
            )

            print(
                f"  topic: "
                f"{event.get('topic')}",
                flush=True
            )

            print(
                f"  partition: "
                f"{event.get('partition')}",
                flush=True
            )

            print(
                f"  offset: "
                f"{event.get('offset')}",
                flush=True
            )

            print(
                f"  tsMs: "
                f"{event.get('tsMs')}",
                flush=True
            )

        parsed_event = parse_debezium_event(
            event,
            file_name,
            line_number
        )

        if parsed_event is None:

            ignored_lines += 1

            if ignored_lines <= 5:

                print(
                    "",
                    flush=True
                )

                print(
                    f"IGNORED JSONL line "
                    f"{line_number}",
                    flush=True
                )

                print(
                    f"  top-level keys: "
                    f"{list(event.keys())}",
                    flush=True
                )

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

        if len(events) <= 3:

            print(
                "",
                flush=True
            )

            print(
                f"PARSED EVENT #{len(events)}",
                flush=True
            )

            print(
                f"  Line      : "
                f"{line_number}",
                flush=True
            )

            print(
                f"  Event ID  : "
                f"{parsed_event['event_id']}",
                flush=True
            )

            print(
                f"  Timestamp : "
                f"{parsed_event['event_timestamp']}",
                flush=True
            )

            print(
                f"  Topic     : "
                f"{parsed_event['topic']}",
                flush=True
            )

            print(
                f"  Partition : "
                f"{parsed_event['partition_number']}",
                flush=True
            )

            print(
                f"  Offset    : "
                f"{parsed_event['kafka_offset']}",
                flush=True
            )

            print(
                f"  Operation : "
                f"{parsed_event['event_type']}",
                flush=True
            )

            print(
                f"  Database  : "
                f"{parsed_event['database_name']}",
                flush=True
            )

            print(
                f"  Schema    : "
                f"{parsed_event['schema_name']}",
                flush=True
            )

            print(
                f"  Table     : "
                f"{parsed_event['table_name']}",
                flush=True
            )

            print(
                f"  Record ID : "
                f"{parsed_event['record_id']}",
                flush=True
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
        f"Total lines          : {total_lines}",
        flush=True
    )

    print(
        f"CDC events parsed    : {len(events)}",
        flush=True
    )

    print(
        f"Invalid JSON lines   : {invalid_lines}",
        flush=True
    )

    print(
        f"Ignored lines        : {ignored_lines}",
        flush=True
    )

    print(
        f"Operations           : {operation_counts}",
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
            f"ERROR decoding JSON "
            f"{file_name}: {e}",
            flush=True
        )

        return []

    if not text:

        print(
            "JSON file is empty.",
            flush=True
        )

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
        f"Objects found        : {len(json_events)}",
        flush=True
    )

    print(
        f"CDC events parsed    : {len(events)}",
        flush=True
    )

    print(
        f"Ignored objects      : {ignored}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    return events


# ============================================================
# PARSE SIMPLE CDC CSV
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

                print(
                    f"WARNING: Unknown event type "
                    f"'{raw_event_type}' "
                    f"on line {line_number}",
                    flush=True
                )

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

            row = {

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
            }

            rows.append(
                row
            )

            if len(rows) <= 3:

                print(
                    "",
                    flush=True
                )

                print(
                    f"PARSED SIMPLE CDC EVENT "
                    f"#{len(rows)}",
                    flush=True
                )

                print(
                    f"  Line      : {line_number}",
                    flush=True
                )

                print(
                    f"  Event ID  : {event_id}",
                    flush=True
                )

                print(
                    f"  Timestamp : {event_timestamp}",
                    flush=True
                )

                print(
                    f"  Operation : {event_type}",
                    flush=True
                )

                print(
                    f"  Table     : {table_name}",
                    flush=True
                )

                print(
                    f"  Record ID : {record_id}",
                    flush=True
                )

        except Exception as e:

            print(
                f"ERROR parsing simple CDC CSV "
                f"{file_name}, line {line_number}: {e}",
                flush=True
            )

            continue

    print(
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        f"Simple CDC CSV parsing complete: {file_name}",
        flush=True
    )

    print(
        f"CDC events parsed: {len(rows)}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    return rows


# ============================================================
# PARSE KAFKA CDC CSV
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

            if not topic:

                topic = None

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

                    print(
                        f"WARNING: Invalid partition "
                        f"'{partition_raw}' "
                        f"line {line_number}",
                        flush=True
                    )

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

                    print(
                        f"WARNING: Invalid offset "
                        f"'{offset_raw}' "
                        f"line {line_number}",
                        flush=True
                    )

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

                print(
                    f"WARNING: Unknown operation "
                    f"'{operation}' "
                    f"line {line_number}",
                    flush=True
                )

                continue

            database_name = (
                csv_row.get(
                    "Database"
                )
                or ""
            ).strip()

            if not database_name:

                database_name = None

            table_name = (
                csv_row.get(
                    "Table"
                )
                or ""
            ).strip()

            if not table_name:

                table_name = None

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

            event_id = line_number

            rows.append({

                "event_id":
                    event_id,

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

            if len(rows) <= 3:

                print(
                    "",
                    flush=True
                )

                print(
                    f"PARSED KAFKA CSV EVENT "
                    f"#{len(rows)}",
                    flush=True
                )

                print(
                    f"  Line      : {line_number}",
                    flush=True
                )

                print(
                    f"  Timestamp : {event_timestamp}",
                    flush=True
                )

                print(
                    f"  Topic     : {topic}",
                    flush=True
                )

                print(
                    f"  Partition : {partition}",
                    flush=True
                )

                print(
                    f"  Offset    : {kafka_offset}",
                    flush=True
                )

                print(
                    f"  Operation : {event_type}",
                    flush=True
                )

                print(
                    f"  Database  : {database_name}",
                    flush=True
                )

                print(
                    f"  Schema    : {schema_name}",
                    flush=True
                )

                print(
                    f"  Table     : {table_name}",
                    flush=True
                )

                print(
                    f"  Record ID : {record_id}",
                    flush=True
                )

        except Exception as e:

            print(
                f"ERROR parsing Kafka CSV "
                f"{file_name}, "
                f"line {line_number}: {e}",
                flush=True
            )

            continue

    operation_counts = {}

    for row in rows:

        operation = row[
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
        f"Kafka CDC CSV parsing complete: {file_name}",
        flush=True
    )

    print(
        f"Total CDC events parsed: {len(rows)}",
        flush=True
    )

    print(
        f"Operations: {operation_counts}",
        flush=True
    )

    print(
        "================================================",
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
            f"ERROR decoding CSV "
            f"{file_name}: {e}",
            flush=True
        )

        return []

    if not text.strip():

        print(
            "CSV file is empty.",
            flush=True
        )

        return []

    csv_stream = StringIO(
        text
    )

    reader = csv.DictReader(
        csv_stream
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

    print(
        f"CSV headers detected: "
        f"{reader.fieldnames}",
        flush=True
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
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        f"ERROR: Unsupported CSV format: "
        f"{file_name}",
        flush=True
    )

    print(
        f"Detected columns: "
        f"{reader.fieldnames}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    return []


# ============================================================
# DELETE OLD EVENTS FOR FILE
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

    deleted_count = cursor.rowcount

    cursor.close()

    return deleted_count


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

        try:

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
                        Json(
                            row.get(
                                "before_data"
                            )
                        )
                        if row.get(
                            "before_data"
                        ) is not None
                        else None
                    ),

                    (
                        Json(
                            row.get(
                                "after_data"
                            )
                        )
                        if row.get(
                            "after_data"
                        ) is not None
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

        except Exception as e:

            print(
                "",
                flush=True
            )

            print(
                "ERROR INSERTING CDC EVENT",
                flush=True
            )

            print(
                f"  Bucket    : {bucket_name}",
                flush=True
            )

            print(
                f"  File      : {file_name}",
                flush=True
            )

            print(
                f"  Event ID  : "
                f"{row.get('event_id')}",
                flush=True
            )

            print(
                f"  Operation : "
                f"{row.get('event_type')}",
                flush=True
            )

            print(
                f"  Error     : {e}",
                flush=True
            )

            raise

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
            etag
        )
    )

    cursor.close()

    return (
        inserted_count,
        skipped_count
    )


# ============================================================
# PROCESS FILE
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
        "================================================",
        flush=True
    )

    print(
        f"PROCESSING FILE: "
        f"{bucket_name}/{file_name}",
        flush=True
    )

    print(
        f"ETag: {etag}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    # ========================================================
    # DOWNLOAD
    # ========================================================

    response = s3.get_object(
        Bucket=bucket_name,
        Key=file_name
    )

    data = response[
        "Body"
    ].read()

    print(
        f"Downloaded {len(data)} bytes.",
        flush=True
    )

    # ========================================================
    # FORMAT
    # ========================================================

    lower_name = file_name.lower()

    if lower_name.endswith(
        ".csv"
    ):

        print(
            "Detected extension: CSV",
            flush=True
        )

        rows = parse_cdc_csv(
            data,
            file_name
        )

    elif lower_name.endswith(
        ".jsonl"
    ):

        print(
            "Detected extension: JSONL / NDJSON",
            flush=True
        )

        rows = parse_jsonl(
            data,
            file_name
        )

    elif lower_name.endswith(
        ".ndjson"
    ):

        print(
            "Detected extension: NDJSON",
            flush=True
        )

        rows = parse_jsonl(
            data,
            file_name
        )

    elif lower_name.endswith(
        ".json"
    ):

        print(
            "Detected extension: JSON / JSONL / NDJSON",
            flush=True
        )

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
        f"Parsed {len(rows)} events.",
        flush=True
    )

    # ========================================================
    # NEVER MARK EMPTY PARSE AS PROCESSED
    # ========================================================

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
            "The file will NOT be marked as processed.",
            flush=True
        )

        print(
            "It will be retried on the next scan.",
            flush=True
        )

        return False

    # ========================================================
    # DATABASE
    # ========================================================

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
            and
            previous_etag != etag
        )

        force_rebuild = FORCE_REPROCESS

        repair_empty_file = (
            previous_etag is not None
            and
            previous_etag == etag
            and
            existing_event_count == 0
        )

        # ====================================================
        # REBUILD WHEN NEEDED
        # ====================================================

        if (
            file_changed
            or force_rebuild
            or repair_empty_file
        ):

            print(
                "",
                flush=True
            )

            print(
                "Rebuilding CDC events for file.",
                flush=True
            )

            if file_changed:

                print(
                    "Reason: ETag changed.",
                    flush=True
                )

            elif force_rebuild:

                print(
                    "Reason: FORCE_REPROCESS=true.",
                    flush=True
                )

            elif repair_empty_file:

                print(
                    "Reason: Previously processed but "
                    "zero CDC rows existed.",
                    flush=True
                )

            deleted_count = delete_file_events(
                conn,
                bucket_name,
                file_name
            )

            print(
                f"Deleted old CDC events: "
                f"{deleted_count}",
                flush=True
            )

        # ====================================================
        # INSERT
        # ====================================================

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

        # ====================================================
        # VERIFY BEFORE COMMIT
        # ====================================================

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
                "Insert completed but database still "
                "contains ZERO CDC events for this file."
            )

        # ====================================================
        # COMMIT
        # ====================================================

        conn.commit()

        # ====================================================
        # SUCCESS
        # ====================================================

        print(
            "",
            flush=True
        )

        print(
            "================================================",
            flush=True
        )

        print(
            f"SUCCESS: "
            f"{bucket_name}/{file_name}",
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

        print(
            "================================================",
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
            f"{bucket_name}/{file_name}: "
            f"{e}",
            flush=True
        )

        raise

    finally:

        conn.close()


# ============================================================
# GET BUCKET LIST
# ============================================================

def get_minio_buckets():

    configured_buckets = get_configured_buckets()

    # ========================================================
    # EXPLICIT BUCKET CONFIGURATION
    # ========================================================

    if configured_buckets:

        print(
            "",
            flush=True
        )

        print(
            "Using explicitly configured MinIO buckets:",
            flush=True
        )

        for bucket in configured_buckets:

            print(
                f"  - {bucket}",
                flush=True
            )

        return configured_buckets

    # ========================================================
    # AUTOMATIC DISCOVERY
    # ========================================================

    print(
        "",
        flush=True
    )

    print(
        "MINIO_BUCKETS is empty.",
        flush=True
    )

    print(
        "Discovering all buckets accessible "
        "by the configured MinIO credentials...",
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
            "  No buckets found.",
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
    processed_this_bucket = 0
    skipped_this_bucket = 0
    failed_this_bucket = 0

    print(
        "",
        flush=True
    )

    print(
        "################################################",
        flush=True
    )

    print(
        f"BUCKET SCAN START: {bucket_name}",
        flush=True
    )

    print(
        f"MinIO prefix: "
        f"{MINIO_PREFIX or '(entire bucket)'}",
        flush=True
    )

    print(
        "################################################",
        flush=True
    )

    # ========================================================
    # CHECK ACCESS
    # ========================================================

    if not check_bucket_access(
        bucket_name
    ):

        print(
            f"Skipping inaccessible bucket: "
            f"{bucket_name}",
            flush=True
        )

        return {
            "objects": 0,
            "supported": 0,
            "processed": 0,
            "skipped": 0,
            "failed": 1,
        }

    # ========================================================
    # PAGINATED OBJECT SCAN
    # ========================================================

    while True:

        request = {

            "Bucket":
                bucket_name,

            "Prefix":
                MINIO_PREFIX
        }

        if continuation_token:

            request[
                "ContinuationToken"
            ] = continuation_token

        response = s3.list_objects_v2(
            **request
        )

        objects = response.get(
            "Contents",
            []
        )

        total_objects += len(
            objects
        )

        for obj in objects:

            file_name = obj[
                "Key"
            ]

            # ------------------------------------------------
            # Ignore directories
            # ------------------------------------------------

            if file_name.endswith(
                "/"
            ):

                continue

            lower_name = file_name.lower()

            # ------------------------------------------------
            # Supported files
            # ------------------------------------------------

            if not (
                lower_name.endswith(
                    ".csv"
                )
                or
                lower_name.endswith(
                    ".json"
                )
                or
                lower_name.endswith(
                    ".jsonl"
                )
                or
                lower_name.endswith(
                    ".ndjson"
                )
            ):

                continue

            supported_files += 1

            # ------------------------------------------------
            # ETAG
            # ------------------------------------------------

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

            # ------------------------------------------------
            # DECIDE
            # ------------------------------------------------

            try:

                process_required = should_process_file(
                    bucket_name,
                    file_name,
                    etag
                )

            except Exception as e:

                failed_this_bucket += 1

                print(
                    "",
                    flush=True
                )

                print(
                    f"ERROR checking "
                    f"{bucket_name}/{file_name}: "
                    f"{e}",
                    flush=True
                )

                continue

            if not process_required:

                skipped_this_bucket += 1

                continue

            # ------------------------------------------------
            # PROCESS
            # ------------------------------------------------

            try:

                success = process_file(
                    bucket_name,
                    file_name,
                    etag
                )

                if success:

                    processed_this_bucket += 1

                else:

                    failed_this_bucket += 1

            except Exception as e:

                failed_this_bucket += 1

                print(
                    "",
                    flush=True
                )

                print(
                    f"ERROR processing "
                    f"{bucket_name}/{file_name}: "
                    f"{e}",
                    flush=True
                )

        # ----------------------------------------------------
        # PAGINATION
        # ----------------------------------------------------

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

    # ========================================================
    # BUCKET SUMMARY
    # ========================================================

    print(
        "",
        flush=True
    )

    print(
        "################################################",
        flush=True
    )

    print(
        f"BUCKET SCAN COMPLETE: {bucket_name}",
        flush=True
    )

    print(
        f"Objects             : "
        f"{total_objects}",
        flush=True
    )

    print(
        f"Supported files     : "
        f"{supported_files}",
        flush=True
    )

    print(
        f"Processed            : "
        f"{processed_this_bucket}",
        flush=True
    )

    print(
        f"Already processed    : "
        f"{skipped_this_bucket}",
        flush=True
    )

    print(
        f"Failed / retry       : "
        f"{failed_this_bucket}",
        flush=True
    )

    print(
        "################################################",
        flush=True
    )

    return {
        "objects":
            total_objects,

        "supported":
            supported_files,

        "processed":
            processed_this_bucket,

        "skipped":
            skipped_this_bucket,

        "failed":
            failed_this_bucket,
    }


# ============================================================
# SCAN ALL MINIO BUCKETS
# ============================================================

def scan_minio():

    print(
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        "MINIO MULTI-BUCKET SCAN START",
        flush=True
    )

    print(
        f"MinIO endpoint: "
        f"{MINIO_ENDPOINT}",
        flush=True
    )

    print(
        f"MinIO prefix: "
        f"{MINIO_PREFIX or '(entire bucket)'}",
        flush=True
    )

    print(
        f"Force reprocess: "
        f"{FORCE_REPROCESS}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    # ========================================================
    # GET BUCKETS
    # ========================================================

    try:

        buckets = get_minio_buckets()

    except Exception as e:

        print(
            "",
            flush=True
        )

        print(
            f"ERROR discovering MinIO buckets: "
            f"{e}",
            flush=True
        )

        return

    # ========================================================
    # NO BUCKETS
    # ========================================================

    if not buckets:

        print(
            "",
            flush=True
        )

        print(
            "WARNING: No MinIO buckets available.",
            flush=True
        )

        return

    # ========================================================
    # GLOBAL COUNTERS
    # ========================================================

    totals = {

        "objects":
            0,

        "supported":
            0,

        "processed":
            0,

        "skipped":
            0,

        "failed":
            0,
    }

    # ========================================================
    # SCAN EACH BUCKET
    # ========================================================

    for bucket_name in buckets:

        try:

            result = scan_bucket(
                bucket_name
            )

            for key in totals:

                totals[
                    key
                ] += result.get(
                    key,
                    0
                )

        except Exception as e:

            totals[
                "failed"
            ] += 1

            print(
                "",
                flush=True
            )

            print(
                f"ERROR scanning bucket "
                f"'{bucket_name}': {e}",
                flush=True
            )

    # ========================================================
    # GLOBAL SUMMARY
    # ========================================================

    print(
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        "MULTI-BUCKET MINIO SCAN COMPLETE",
        flush=True
    )

    print(
        f"Buckets scanned      : "
        f"{len(buckets)}",
        flush=True
    )

    print(
        f"Objects              : "
        f"{totals['objects']}",
        flush=True
    )

    print(
        f"Supported files      : "
        f"{totals['supported']}",
        flush=True
    )

    print(
        f"Processed this scan  : "
        f"{totals['processed']}",
        flush=True
    )

    print(
        f"Already processed    : "
        f"{totals['skipped']}",
        flush=True
    )

    print(
        f"Failed / retry       : "
        f"{totals['failed']}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        "CDC PYTHON MULTI-BUCKET WORKER",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        f"MinIO endpoint : "
        f"{MINIO_ENDPOINT}",
        flush=True
    )

    # ========================================================
    # BUCKET MODE
    # ========================================================

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
        f"Poll interval  : "
        f"{POLL_INTERVAL} seconds",
        flush=True
    )

    print(
        "Supported files: "
        "CSV / JSON / JSONL / NDJSON",
        flush=True
    )

    print(
        "Supported CSV: "
        "Simple CDC + Kafka CDC",
        flush=True
    )

    print(
        f"Force reprocess: "
        f"{FORCE_REPROCESS}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    # ========================================================
    # DATABASE INITIALIZATION
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
                f"Database initialization failed: "
                f"{e}",
                flush=True
            )

            print(
                "Retrying in 5 seconds...",
                flush=True
            )

            time.sleep(
                5
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
                f"ERROR during MinIO scan: "
                f"{e}",
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
