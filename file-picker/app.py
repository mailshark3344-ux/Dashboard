import os

from flask import Flask, render_template, request, redirect
import boto3
from botocore.client import Config
from urllib.parse import quote


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# MINIO CONFIGURATION
#
# IMPORTANT:
#
# There is intentionally NO MINIO_BUCKET here.
#
# The user selects the bucket dynamically.
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://cdc-minio:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin123"
)


# ============================================================
# REDASH
# ============================================================

REDASH_URL = os.getenv(
    "REDASH_URL",
    "http://127.0.0.1:5000/dashboards/1-dashboard"
)


# ============================================================
# S3 / MINIO CLIENT
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(
        signature_version="s3v4"
    ),
    region_name="us-east-1"
)


# ============================================================
# LIST BUCKETS
# ============================================================

def list_buckets():

    buckets = []

    try:

        response = s3.list_buckets()

        for bucket in response.get(
            "Buckets",
            []
        ):

            bucket_name = bucket.get(
                "Name"
            )

            if not bucket_name:
                continue

            buckets.append({
                "name": bucket_name,
                "created": bucket.get(
                    "CreationDate"
                )
            })

    except Exception as e:

        print(
            f"ERROR listing MinIO buckets: {e}",
            flush=True
        )

        raise

    buckets.sort(
        key=lambda x: x["name"].lower()
    )

    return buckets


# ============================================================
# CHECK BUCKET ACCESS
# ============================================================

def bucket_exists(
    bucket
):

    if not bucket:

        return False

    try:

        s3.head_bucket(
            Bucket=bucket
        )

        return True

    except Exception as e:

        print(
            f"Bucket access check failed "
            f"for '{bucket}': {e}",
            flush=True
        )

        return False


# ============================================================
# LIST OBJECTS
#
# Lists folders and files directly inside the requested
# MinIO prefix.
#
# Example:
#
# bucket:
#
# customer-a
#
# prefix:
#
# cdc/
#
# ============================================================

def list_objects(
    bucket,
    prefix=""
):

    folders = []
    files = []

    if not bucket:

        return folders, files

    # --------------------------------------------------------
    # Verify bucket
    # --------------------------------------------------------

    if not bucket_exists(
        bucket
    ):

        raise ValueError(
            f"Bucket '{bucket}' does not exist "
            f"or is not accessible."
        )

    # --------------------------------------------------------
    # Normalize prefix
    # --------------------------------------------------------

    if prefix is None:

        prefix = ""

    prefix = str(
        prefix
    )

    # --------------------------------------------------------
    # Ensure folder prefix ends with /
    # --------------------------------------------------------

    if prefix and not prefix.endswith(
        "/"
    ):

        prefix += "/"

    # --------------------------------------------------------
    # Get objects using delimiter
    # --------------------------------------------------------

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    pages = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/"
    )

    for page in pages:

        # ====================================================
        # FOLDERS
        # ====================================================

        for item in page.get(
            "CommonPrefixes",
            []
        ):

            folder_prefix = item.get(
                "Prefix"
            )

            if not folder_prefix:
                continue

            folder_name = (
                folder_prefix[len(prefix):]
                .rstrip("/")
            )

            if folder_name:

                folders.append({
                    "name": folder_name,
                    "prefix": folder_prefix
                })

        # ====================================================
        # FILES
        # ====================================================

        for item in page.get(
            "Contents",
            []
        ):

            key = item.get(
                "Key"
            )

            if not key:
                continue

            # ------------------------------------------------
            # Ignore folder placeholder objects
            # ------------------------------------------------

            if key.endswith(
                "/"
            ):

                continue

            # ------------------------------------------------
            # Ignore prefix itself
            # ------------------------------------------------

            if key == prefix:

                continue

            # ------------------------------------------------
            # Relative filename
            # ------------------------------------------------

            relative_name = key[
                len(prefix):
            ]

            if not relative_name:

                continue

            # ------------------------------------------------
            # Only files directly inside current folder
            # ------------------------------------------------

            if "/" in relative_name:

                continue

            files.append({
                "key": key,
                "name": relative_name,
                "size": item.get(
                    "Size",
                    0
                ),
                "last_modified": item.get(
                    "LastModified"
                )
            })

    # ========================================================
    # REMOVE DUPLICATES
    # ========================================================

    unique_folders = {}

    for folder in folders:

        unique_folders[
            folder["prefix"]
        ] = folder

    unique_files = {}

    for file in files:

        unique_files[
            file["key"]
        ] = file

    folders = list(
        unique_folders.values()
    )

    files = list(
        unique_files.values()
    )

    # ========================================================
    # SORT
    # ========================================================

    folders.sort(
        key=lambda x: x["name"].lower()
    )

    files.sort(
        key=lambda x: x["name"].lower()
    )

    return folders, files


# ============================================================
# ROOT
# ============================================================

@app.route("/")
def index():

    return redirect(
        "/select"
    )


# ============================================================
# SELECT
#
# Two modes:
#
# 1. No bucket:
#
#       /select
#
#    Show available buckets.
#
# 2. Bucket selected:
#
#       /select?bucket=mybucket
#
#    Show folders/files inside bucket.
# ============================================================

@app.route("/select")
def select_file():

    bucket = request.args.get(
        "bucket",
        ""
    ).strip()

    prefix = request.args.get(
        "prefix",
        ""
    )

    # ========================================================
    # MODE 1
    #
    # No bucket selected.
    #
    # Show buckets.
    # ========================================================

    if not bucket:

        try:

            buckets = list_buckets()

        except Exception as e:

            return (
                f"Unable to list MinIO buckets: {e}",
                500
            )

        return render_template(
            "select.html",
            mode="buckets",
            bucket=None,
            prefix="",
            buckets=buckets,
            folders=[],
            files=[],
            parent_prefix=None
        )

    # ========================================================
    # VERIFY BUCKET
    # ========================================================

    if not bucket_exists(
        bucket
    ):

        return (
            f"Bucket '{bucket}' does not exist "
            f"or is not accessible.",
            404
        )

    # ========================================================
    # MODE 2
    #
    # Bucket selected.
    #
    # Browse objects.
    # ========================================================

    try:

        folders, files = list_objects(
            bucket,
            prefix
        )

    except Exception as e:

        return (
            f"Unable to read bucket "
            f"'{bucket}': {e}",
            500
        )

    # ========================================================
    # CALCULATE PARENT FOLDER
    # ========================================================

    parent_prefix = None

    if prefix:

        clean_prefix = prefix.rstrip(
            "/"
        )

        if "/" in clean_prefix:

            parent_prefix = (
                clean_prefix.rsplit(
                    "/",
                    1
                )[0]
                + "/"
            )

        else:

            parent_prefix = ""

    # ========================================================
    # RENDER
    # ========================================================

    return render_template(
        "select.html",

        mode="files",

        bucket=bucket,

        prefix=prefix,

        buckets=[],

        folders=folders,

        files=files,

        parent_prefix=parent_prefix
    )


# ============================================================
# CHOOSE FILE
#
# Sends BOTH:
#
#   p_source_bucket
#   p_source_file
#
# to Redash.
# ============================================================

@app.route("/choose")
def choose_file():

    bucket = request.args.get(
        "bucket",
        ""
    ).strip()

    selected_file = request.args.get(
        "file",
        ""
    ).strip()

    # ========================================================
    # VALIDATION
    # ========================================================

    if not bucket:

        return (
            "No bucket selected.",
            400
        )

    if not selected_file:

        return (
            "No file selected.",
            400
        )

    # ========================================================
    # VERIFY BUCKET
    # ========================================================

    if not bucket_exists(
        bucket
    ):

        return (
            f"Bucket '{bucket}' does not exist "
            f"or is not accessible.",
            404
        )

    # ========================================================
    # VERIFY OBJECT
    #
    # This prevents sending a random/nonexistent object
    # to Redash.
    # ========================================================

    try:

        s3.head_object(
            Bucket=bucket,
            Key=selected_file
        )

    except Exception as e:

        print(
            f"ERROR checking object "
            f"'{bucket}/{selected_file}': {e}",
            flush=True
        )

        return (
            f"File '{selected_file}' was not found "
            f"in bucket '{bucket}'.",
            404
        )

    # ========================================================
    # URL ENCODING
    #
    # Encode the complete bucket and object key.
    # ========================================================

    encoded_bucket = quote(
        bucket,
        safe=""
    )

    encoded_file = quote(
        selected_file,
        safe=""
    )

    # ========================================================
    # REDASH REDIRECT
    #
    # Example:
    #
    # /dashboards/1-dashboard
    #     ?p_source_bucket=customer_a
    #     &p_source_file=cdc/data/file.csv
    #
    # ========================================================

    separator = (
        "&"
        if "?" in REDASH_URL
        else "?"
    )

    redirect_url = (
        f"{REDASH_URL}"
        f"{separator}"
        f"p_source_bucket={encoded_bucket}"
        f"&p_source_file={encoded_file}"
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
        "FILE SELECTED",
        flush=True
    )

    print(
        f"Bucket : {bucket}",
        flush=True
    )

    print(
        f"File   : {selected_file}",
        flush=True
    )

    print(
        f"Redirect: {redirect_url}",
        flush=True
    )

    print(
        "================================================",
        flush=True
    )

    return redirect(
        redirect_url
    )


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    minio_status = "connected"

    buckets = []

    try:

        buckets = list_buckets()

    except Exception as e:

        minio_status = (
            f"error: {str(e)}"
        )

    return {
        "status": "ok",

        "minio": MINIO_ENDPOINT,

        "minio_status": minio_status,

        "bucket_count": len(
            buckets
        ),

        "buckets": [
            bucket["name"]
            for bucket in buckets
        ],

        "redash": REDASH_URL
    }


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=7000
    )
