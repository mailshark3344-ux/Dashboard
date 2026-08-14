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

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "myfiles"
)

MINIO_PREFIX = os.getenv(
    "MINIO_PREFIX",
    "cdc/"
)

POLL_INTERVAL = int(
    os.getenv(
        "POLL_INTERVAL",
        "10"
    )
)

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

# ------------------------------------------------------------
# FORCE REPROCESS
#
# Set:
#
# FORCE_REPROCESS=true
#
# if you want every supported MinIO file to be rebuilt on
# every scan.
#
# Default is false.
#
# IMPORTANT:
# Even when false, this worker automatically repairs a file
# when processed_files says it was processed but cdc_events
# contains ZERO events for that file.
# ------------------------------------------------------------

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

        # ----------------------------------------------------
        # PROCESSED FILES
        # ----------------------------------------------------

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_files (
                file_name TEXT PRIMARY KEY,
                file_etag TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        # ----------------------------------------------------
        # CDC EVENTS
        # ----------------------------------------------------

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

                source_file TEXT,
                source_line_number INTEGER,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        # ----------------------------------------------------
        # MIGRATION COLUMNS
        # ----------------------------------------------------

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

            cursor.execute(statement)

        # ----------------------------------------------------
        # INDEXES
        # ----------------------------------------------------

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
            idx_cdc_events_topic_partition_offset
            ON cdc_events(
                topic,
                partition_number,
                kafka_offset
            );
            """
        )

        # ----------------------------------------------------
        # REMOVE OLD UNIQUE INDEX
        # ----------------------------------------------------

        cursor.execute(
            """
            DROP INDEX IF EXISTS
            uq_cdc_event_source_file_event_id;
            """
        )

        # ----------------------------------------------------
        # KAFKA EVENT UNIQUE INDEX
        # ----------------------------------------------------

        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
            uq_cdc_event_file_topic_partition_offset
            ON cdc_events(
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

        # ----------------------------------------------------
        # NON-KAFKA FALLBACK UNIQUE INDEX
        # ----------------------------------------------------

        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
            uq_cdc_event_file_event_id_fallback
            ON cdc_events(
                source_file,
                event_id
            )
            WHERE
                topic IS NULL
                OR partition_number IS NULL
                OR kafka_offset IS NULL;
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
            WHERE file_name = %s
            """,
            (
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
    file_name
):

    info = get_processed_file_info(
        file_name
    )

    if info is None:

        return None

    return info["etag"]


# ============================================================
# COUNT EVENTS FOR FILE
#
# This is the important repair check.
#
# A file may exist in processed_files because an older version
# of the worker marked it processed even though parsing produced
# zero events.
#
# In that case the new worker MUST process it again.
# ============================================================

def count_events_for_file(
    file_name
):

    conn = get_db_connection()

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM cdc_events
            WHERE source_file = %s
            """,
            (
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
#
# Returns:
#
# True  -> process
# False -> skip
#
# Rules:
#
# 1. FORCE_REPROCESS=true -> process
#
# 2. File never processed -> process
#
# 3. ETag changed -> process
#
# 4. Same ETag + zero CDC rows -> process
#
# 5. Same ETag + CDC rows exist -> skip
# ============================================================

def should_process_file(
    file_name,
    etag
):

    print(
        "",
        flush=True
    )

    print(
        f"Checking: {file_name}",
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
        file_name
    )

    # --------------------------------------------------------
    # Never processed
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
    # Changed file
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
    # Same ETag.
    #
    # Now verify that the file actually produced events.
    # --------------------------------------------------------

    event_count = count_events_for_file(
        file_name
    )

    print(
        f"  Existing CDC events: {event_count}",
        flush=True
    )

    # --------------------------------------------------------
    # IMPORTANT REPAIR CONDITION
    # --------------------------------------------------------

    if event_count == 0:

        print(
            "PROCESS: File is marked processed but "
            "has ZERO CDC events.",
            flush=True
        )

        print(
            "PROCESS: Reprocessing file to repair "
            "previous processing result.",
            flush=True
        )

        return True

    # --------------------------------------------------------
    # Normal skip
    # --------------------------------------------------------

    print(
        f"SKIP: {file_name}",
        flush=True
    )

    print(
        f"  Already processed with ETag: "
        f"{previous_etag}",
        flush=True
    )

    print(
        f"  CDC events already stored: "
        f"{event_count}",
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
    # ISO timestamp
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

    # --------------------------------------------------------
    # Fallback first value
    # --------------------------------------------------------

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
#
# postgres-connecter.public.employees
#
# connector = postgres-connecter
# schema    = public
# table     = employees
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
# EXTRACT DEBEZIUM PAYLOAD
#
# Supports all of these:
#
# A:
# {
#   "value": {
#       "schema": {...},
#       "payload": {...}
#   }
# }
#
# B:
# {
#   "value": {
#       "before": ...,
#       "after": ...,
#       "op": "c"
#   }
# }
#
# C:
# {
#   "payload": {
#       "before": ...,
#       "after": ...,
#       "op": "c"
#   }
# }
#
# D:
# {
#   "before": ...,
#   "after": ...,
#   "op": "c"
# }
#
# E:
# {
#   "value": {
#       "schema": ...,
#       "payload": {
#           ...
#       }
#   }
# }
# ============================================================

def get_payload(
    event
):

    if not isinstance(
        event,
        dict
    ):

        return None

    # --------------------------------------------------------
    # FIRST: value
    # --------------------------------------------------------

    value = event.get(
        "value"
    )

    if isinstance(
        value,
        dict
    ):

        # ----------------------------------------------------
        # Kafka Connect:
        #
        # value.payload
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Value itself is Debezium envelope
        # ----------------------------------------------------

        if (
            "op" in value
            or
            "before" in value
            or
            "after" in value
        ):

            return value

    # --------------------------------------------------------
    # SECOND: direct payload
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # THIRD: direct Debezium object
    # --------------------------------------------------------

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
# EXTRACT EVENT METADATA
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

    # --------------------------------------------------------
    # Partition
    # --------------------------------------------------------

    try:

        if partition is not None:

            partition = int(
                partition
            )

    except Exception:

        partition = None

    # --------------------------------------------------------
    # Kafka offset
    # --------------------------------------------------------

    try:

        if kafka_offset is not None:

            kafka_offset = int(
                kafka_offset
            )

    except Exception:

        kafka_offset = None

    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # BEFORE / AFTER
    # --------------------------------------------------------

    before_data = payload.get(
        "before"
    )

    after_data = payload.get(
        "after"
    )

    # --------------------------------------------------------
    # OPERATION
    # --------------------------------------------------------

    op = payload.get(
        "op"
    )

    event_type = convert_debezium_operation(
        op
    )

    if event_type == "UNKNOWN":

        return None

    # --------------------------------------------------------
    # EVENT METADATA
    # --------------------------------------------------------

    (
        topic,
        partition,
        kafka_offset,
        envelope_timestamp_ms
    ) = get_event_metadata(
        event
    )

    # --------------------------------------------------------
    # SOURCE
    # --------------------------------------------------------

    source = payload.get(
        "source"
    )

    if not isinstance(
        source,
        dict
    ):

        source = {}

    # --------------------------------------------------------
    # DATABASE
    # --------------------------------------------------------

    database_name = source.get(
        "db"
    )

    # --------------------------------------------------------
    # SCHEMA
    # --------------------------------------------------------

    schema_name = source.get(
        "schema"
    )

    # --------------------------------------------------------
    # TABLE
    # --------------------------------------------------------

    table_name = source.get(
        "table"
    )

    if not table_name:

        table_name = "unknown"

    # --------------------------------------------------------
    # TOPIC FALLBACK
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # TOPIC CAN ALSO SUPPLY SCHEMA/TABLE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # EVENT TIMESTAMP
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # SNAPSHOT
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # SOURCE LSN
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # SOURCE TXID
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # RECORD ID
    # --------------------------------------------------------

    record_id = find_record_id(
        after_data,
        before_data,
        table_name
    )

    # --------------------------------------------------------
    # EVENT ID
    #
    # Source line is unique within the source file.
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Process each physical line independently.
    # --------------------------------------------------------

    for line_number, raw_line in enumerate(
        text.splitlines(),
        start=1
    ):

        total_lines += 1

        line = raw_line.strip()

        if not line:

            continue

        # ----------------------------------------------------
        # JSON parse
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # FIRST RECORD DEBUG
        # ----------------------------------------------------

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

            value = event.get(
                "value"
            )

            print(
                f"  value type: "
                f"{type(value).__name__}",
                flush=True
            )

            if isinstance(
                value,
                dict
            ):

                print(
                    f"  value keys: "
                    f"{list(value.keys())}",
                    flush=True
                )

                payload = value.get(
                    "payload"
                )

                if isinstance(
                    payload,
                    dict
                ):

                    print(
                        f"  payload keys: "
                        f"{list(payload.keys())}",
                        flush=True
                    )

                    print(
                        f"  payload op: "
                        f"{payload.get('op')}",
                        flush=True
                    )

                    print(
                        f"  payload source: "
                        f"{payload.get('source')}",
                        flush=True
                    )

        # ----------------------------------------------------
        # Parse Debezium
        # ----------------------------------------------------

        parsed_event = parse_debezium_event(
            event,
            file_name,
            line_number
        )

        if parsed_event is None:

            ignored_lines += 1

            # Print first few ignored records to diagnose
            # unexpected Kafka Connect structures.

            if ignored_lines <= 5:

                value = event.get(
                    "value"
                )

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

                if isinstance(
                    value,
                    dict
                ):

                    print(
                        f"  value keys: "
                        f"{list(value.keys())}",
                        flush=True
                    )

                    payload = value.get(
                        "payload"
                    )

                    if isinstance(
                        payload,
                        dict
                    ):

                        print(
                            f"  payload keys: "
                            f"{list(payload.keys())}",
                            flush=True
                        )

                        print(
                            f"  payload op: "
                            f"{payload.get('op')}",
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

        # ----------------------------------------------------
        # Debug first 3 events
        # ----------------------------------------------------

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
                f"  Before    : "
                f"{parsed_event['before_data']}",
                flush=True
            )

            print(
                f"  After     : "
                f"{parsed_event['after_data']}",
                flush=True
            )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Try normal JSON first.
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # JSON array
    # --------------------------------------------------------

    if isinstance(
        parsed,
        list
    ):

        json_events = parsed

    # --------------------------------------------------------
    # Single object
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Normalize headers
    # --------------------------------------------------------

    reader.fieldnames = [

        header.strip()
        if header is not None
        else None

        for header in reader.fieldnames

    ]

    print(
        f"CSV headers detected: "
        f"{reader.fieldnames}",
        flush=True
    )

    required_columns = [

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
    ]

    missing_columns = [

        column
        for column in required_columns
        if column not in reader.fieldnames

    ]

    if missing_columns:

        print(
            "",
            flush=True
        )

        print(
            "================================================",
            flush=True
        )

        print(
            f"ERROR: Invalid CSV format: "
            f"{file_name}",
            flush=True
        )

        print(
            f"Missing columns: "
            f"{missing_columns}",
            flush=True
        )

        print(
            f"Expected columns: "
            f"{required_columns}",
            flush=True
        )

        print(
            f"Actual columns: "
            f"{reader.fieldnames}",
            flush=True
        )

        print(
            "================================================",
            flush=True
        )

        return []

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

            # ------------------------------------------------
            # Empty row
            # ------------------------------------------------

            if all(
                value is None
                or not str(value).strip()
                for value in csv_row.values()
            ):

                continue

            # ------------------------------------------------
            # TIMESTAMP
            # ------------------------------------------------

            event_timestamp = parse_csv_timestamp(
                csv_row.get(
                    "Timestamp (UTC)"
                )
            )

            # ------------------------------------------------
            # TOPIC
            # ------------------------------------------------

            topic = (
                csv_row.get(
                    "Topic"
                )
                or ""
            ).strip()

            if not topic:

                topic = None

            # ------------------------------------------------
            # PARTITION
            # ------------------------------------------------

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

            # ------------------------------------------------
            # OFFSET
            # ------------------------------------------------

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

            # ------------------------------------------------
            # OPERATION
            # ------------------------------------------------

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

            # ------------------------------------------------
            # DATABASE
            # ------------------------------------------------

            database_name = (
                csv_row.get(
                    "Database"
                )
                or ""
            ).strip()

            if not database_name:

                database_name = None

            # ------------------------------------------------
            # TABLE
            # ------------------------------------------------

            table_name = (
                csv_row.get(
                    "Table"
                )
                or ""
            ).strip()

            if not table_name:

                table_name = None

            # ------------------------------------------------
            # KEY
            # ------------------------------------------------

            key_data = safe_json_value(
                csv_row.get(
                    "Key"
                )
            )

            # ------------------------------------------------
            # BEFORE
            # ------------------------------------------------

            before_data = safe_json_value(
                csv_row.get(
                    "Before"
                )
            )

            # ------------------------------------------------
            # AFTER
            # ------------------------------------------------

            after_data = safe_json_value(
                csv_row.get(
                    "After"
                )
            )

            # ------------------------------------------------
            # TOPIC SCHEMA/TABLE
            # ------------------------------------------------

            schema_name, topic_table = parse_topic(
                topic,
                database_name,
                table_name
            )

            if not table_name:

                table_name = topic_table

            if not table_name:

                table_name = "unknown"

            # ------------------------------------------------
            # RECORD ID
            # ------------------------------------------------

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

            # ------------------------------------------------
            # ROW
            # ------------------------------------------------

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

            if len(rows) <= 3:

                print(
                    "",
                    flush=True
                )

                print(
                    f"PARSED CSV EVENT "
                    f"#{len(rows)}",
                    flush=True
                )

                print(
                    f"  Line      : "
                    f"{line_number}",
                    flush=True
                )

                print(
                    f"  Timestamp : "
                    f"{event_timestamp}",
                    flush=True
                )

                print(
                    f"  Topic     : "
                    f"{topic}",
                    flush=True
                )

                print(
                    f"  Partition : "
                    f"{partition}",
                    flush=True
                )

                print(
                    f"  Offset    : "
                    f"{kafka_offset}",
                    flush=True
                )

                print(
                    f"  Operation : "
                    f"{event_type}",
                    flush=True
                )

                print(
                    f"  Database  : "
                    f"{database_name}",
                    flush=True
                )

                print(
                    f"  Schema    : "
                    f"{schema_name}",
                    flush=True
                )

                print(
                    f"  Table     : "
                    f"{table_name}",
                    flush=True
                )

                print(
                    f"  Record ID : "
                    f"{record_id}",
                    flush=True
                )

        except Exception as e:

            print(
                f"ERROR parsing CSV "
                f"{file_name}, "
                f"line {line_number}: "
                f"{e}",
                flush=True
            )

            continue

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

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
        f"CSV parsing complete: {file_name}",
        flush=True
    )

    print(
        f"Total CDC events parsed: "
        f"{len(rows)}",
        flush=True
    )

    print(
        f"Operations: "
        f"{operation_counts}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    return rows


# ============================================================
# DELETE OLD EVENTS FOR FILE
# ============================================================

def delete_file_events(
    conn,
    file_name
):

    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM cdc_events
        WHERE source_file = %s
        """,
        (
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
                f"  Topic     : "
                f"{row.get('topic')}",
                flush=True
            )

            print(
                f"  Partition : "
                f"{row.get('partition_number')}",
                flush=True
            )

            print(
                f"  Offset    : "
                f"{row.get('kafka_offset')}",
                flush=True
            )

            print(
                f"  Error     : {e}",
                flush=True
            )

            raise

    # --------------------------------------------------------
    # Mark file processed
    # --------------------------------------------------------

    cursor.execute(
        """
        INSERT INTO processed_files (
            file_name,
            file_etag,
            processed_at
        )
        VALUES (
            %s,
            %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (file_name)
        DO UPDATE SET

            file_etag =
                EXCLUDED.file_etag,

            processed_at =
                CURRENT_TIMESTAMP
        """,
        (
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
        f"PROCESSING FILE: {file_name}",
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

    # --------------------------------------------------------
    # DOWNLOAD
    # --------------------------------------------------------

    response = s3.get_object(
        Bucket=MINIO_BUCKET,
        Key=file_name
    )

    data = response[
        "Body"
    ].read()

    print(
        f"Downloaded {len(data)} bytes.",
        flush=True
    )

    # --------------------------------------------------------
    # FORMAT
    # --------------------------------------------------------

    lower_name = file_name.lower()

    if lower_name.endswith(
        ".csv"
    ):

        print(
            "Detected format: CSV",
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
            "Detected format: JSONL / NDJSON",
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
            "Detected format: NDJSON",
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
            "Detected format: JSON / JSONL / NDJSON",
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

    # --------------------------------------------------------
    # NEVER MARK AN EMPTY PARSE AS PROCESSED
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # DATABASE
    # --------------------------------------------------------

    conn = get_db_connection()

    try:

        previous_etag = get_processed_etag(
            file_name
        )

        # ----------------------------------------------------
        # Determine whether existing data needs replacement.
        #
        # If the file is being repaired because processed_files
        # existed but CDC rows were zero, this DELETE is harmless.
        # ----------------------------------------------------

        existing_event_count = count_events_for_file(
            file_name
        )

        file_changed = (
            previous_etag is not None
            and
            previous_etag != etag
        )

        force_rebuild = (
            FORCE_REPROCESS
        )

        repair_empty_file = (
            previous_etag is not None
            and
            previous_etag == etag
            and
            existing_event_count == 0
        )

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
                    "Reason: File was previously marked "
                    "processed but contains zero CDC rows.",
                    flush=True
                )

            deleted_count = delete_file_events(
                conn,
                file_name
            )

            print(
                f"Deleted old CDC events: "
                f"{deleted_count}",
                flush=True
            )

        # ----------------------------------------------------
        # INSERT
        # ----------------------------------------------------

        (
            inserted_count,
            skipped_count
        ) = insert_events(
            conn,
            rows,
            file_name,
            etag
        )

        conn.commit()

        # ----------------------------------------------------
        # Verify database result
        # ----------------------------------------------------

        final_count = count_events_for_file(
            file_name
        )

        if final_count == 0:

            raise RuntimeError(
                "Insert completed but database still "
                "contains ZERO CDC events for this file."
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
            f"SUCCESS: {file_name}",
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
            f"ERROR processing {file_name}: "
            f"{e}",
            flush=True
        )

        raise

    finally:

        conn.close()


# ============================================================
# SCAN MINIO
# ============================================================

def scan_minio():

    continuation_token = None

    total_objects = 0

    supported_files = 0

    processed_this_scan = 0

    skipped_this_scan = 0

    failed_this_scan = 0

    print(
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        "SCAN START",
        flush=True
    )

    print(
        f"Force reprocess this scan: "
        f"{FORCE_REPROCESS}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    while True:

        request = {

            "Bucket":
                MINIO_BUCKET,

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
            # Supported formats
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
            # ETag
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
            # DECIDE WHETHER TO PROCESS
            # ------------------------------------------------

            try:

                process_required = should_process_file(
                    file_name,
                    etag
                )

            except Exception as e:

                failed_this_scan += 1

                print(
                    "",
                    flush=True
                )

                print(
                    f"ERROR checking {file_name}: "
                    f"{e}",
                    flush=True
                )

                continue

            if not process_required:

                skipped_this_scan += 1

                continue

            # ------------------------------------------------
            # PROCESS
            # ------------------------------------------------

            try:

                success = process_file(
                    file_name,
                    etag
                )

                if success:

                    processed_this_scan += 1

                else:

                    failed_this_scan += 1

            except Exception as e:

                failed_this_scan += 1

                print(
                    "",
                    flush=True
                )

                print(
                    f"ERROR processing "
                    f"{file_name}: {e}",
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

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    print(
        "",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    print(
        "MinIO scan complete.",
        flush=True
    )

    print(
        f"Objects              : "
        f"{total_objects}",
        flush=True
    )

    print(
        f"Supported files      : "
        f"{supported_files}",
        flush=True
    )

    print(
        f"Processed this scan  : "
        f"{processed_this_scan}",
        flush=True
    )

    print(
        f"Already processed    : "
        f"{skipped_this_scan}",
        flush=True
    )

    print(
        f"Failed / retry       : "
        f"{failed_this_scan}",
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
        "CDC PYTHON WORKER",
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

    print(
        f"MinIO bucket   : "
        f"{MINIO_BUCKET}",
        flush=True
    )

    print(
        f"MinIO prefix   : "
        f"{MINIO_PREFIX}",
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
        f"Force reprocess: "
        f"{FORCE_REPROCESS}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    # --------------------------------------------------------
    # DATABASE INITIALIZATION
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # CONTINUOUS POLLING
    # --------------------------------------------------------

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
