import os
from flask import Flask, render_template, request, redirect
import boto3
from botocore.client import Config
from urllib.parse import quote

app = Flask(__name__)

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

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "myfiles"
)

REDASH_URL = os.getenv(
    "REDASH_URL",
    "http://127.0.0.1:5000/dashboards/1-dashboard"
)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)


def list_objects(prefix=""):
    """
    List folders and files directly inside the requested
    MinIO prefix.

    This works for BOTH:

        myfiles/
            cdc_003.csv

    and:

        myfiles/
            cdc data/
                cdc_003.csv
    """

    folders = []
    files = []

    # --------------------------------------------------
    # Get objects using delimiter
    # --------------------------------------------------

    paginator = s3.get_paginator("list_objects_v2")

    pages = paginator.paginate(
        Bucket=MINIO_BUCKET,
        Prefix=prefix,
        Delimiter="/"
    )

    for page in pages:

        # --------------------------------------------------
        # FOLDERS
        # --------------------------------------------------

        for item in page.get("CommonPrefixes", []):

            folder_prefix = item["Prefix"]

            folder_name = (
                folder_prefix[len(prefix):]
                .rstrip("/")
            )

            if folder_name:

                folders.append({
                    "name": folder_name,
                    "prefix": folder_prefix
                })

        # --------------------------------------------------
        # FILES
        # --------------------------------------------------

        for item in page.get("Contents", []):

            key = item["Key"]

            # Ignore folder placeholder objects
            if key.endswith("/"):
                continue

            # Ignore the prefix itself
            if key == prefix:
                continue

            # Because Delimiter="/" is used, these are
            # files directly inside the current prefix.
            relative_name = key[len(prefix):]

            if not relative_name:
                continue

            # Extra safety: only display files directly
            # inside the current location.
            if "/" in relative_name:
                continue

            files.append({
                "key": key,
                "name": relative_name,
                "size": item["Size"]
            })

    # --------------------------------------------------
    # Remove duplicates
    # --------------------------------------------------

    unique_folders = {}

    for folder in folders:
        unique_folders[folder["prefix"]] = folder

    unique_files = {}

    for file in files:
        unique_files[file["key"]] = file

    folders = list(unique_folders.values())
    files = list(unique_files.values())

    # --------------------------------------------------
    # Sort
    # --------------------------------------------------

    folders.sort(
        key=lambda x: x["name"].lower()
    )

    files.sort(
        key=lambda x: x["name"].lower()
    )

    return folders, files


@app.route("/")
def index():

    return redirect("/select")


@app.route("/select")
def select_file():

    prefix = request.args.get(
        "prefix",
        ""
    )

    folders, files = list_objects(prefix)

    # --------------------------------------------------
    # Calculate parent folder
    # --------------------------------------------------

    parent_prefix = None

    if prefix:

        clean_prefix = prefix.rstrip("/")

        if "/" in clean_prefix:

            parent_prefix = (
                clean_prefix.rsplit("/", 1)[0]
                + "/"
            )

        else:

            parent_prefix = ""

    # --------------------------------------------------
    # Render picker
    # --------------------------------------------------

    return render_template(
        "select.html",
        prefix=prefix,
        folders=folders,
        files=files,
        parent_prefix=parent_prefix
    )


@app.route("/choose")
def choose_file():

    selected_file = request.args.get(
        "file"
    )

    if not selected_file:

        return "No file selected", 400

    # --------------------------------------------------
    # Encode the COMPLETE MinIO object key
    # --------------------------------------------------

    encoded_file = quote(
        selected_file,
        safe=""
    )

    # --------------------------------------------------
    # Return to Redash
    # --------------------------------------------------

    separator = "&" if "?" in REDASH_URL else "?"

    redirect_url = (
        f"{REDASH_URL}"
        f"{separator}"
        f"p_source_file={encoded_file}"
    )

    return redirect(
        redirect_url
    )


@app.route("/health")
def health():

    try:

        s3.head_bucket(
            Bucket=MINIO_BUCKET
        )

        minio_status = "connected"

    except Exception as e:

        minio_status = f"error: {str(e)}"

    return {
        "status": "ok",
        "minio": MINIO_ENDPOINT,
        "bucket": MINIO_BUCKET,
        "minio_status": minio_status,
        "redash": REDASH_URL
    }


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=7000
    )
