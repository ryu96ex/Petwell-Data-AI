import os
import datetime
import logging

from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import storage

import google.auth
import google.auth.transport.requests

import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
CORS(app, origins=["http://localhost:5173"])

BUCKET_NAME = os.environ["BUCKET_NAME"]

# Lazily initialize credentials to avoid extra work/memory at import time.
_cached_credentials = None


def get_credentials():
    global _cached_credentials

    if _cached_credentials is None or not _cached_credentials.valid:
        credentials, _project_id = google.auth.default()
        auth_request = google.auth.transport.requests.Request()
        credentials.refresh(auth_request)
        _cached_credentials = credentials

    return _cached_credentials


#DB connection pool
# --- 1. CLOUD SQL DATABASE SETUP ---
def init_connection_pool() -> sqlalchemy.engine.Engine:
    # We will pass these via Cloud Run Environment Variables later!
    instance_connection_name = os.environ.get("DB_INSTANCE_CONNECTION_NAME")
    db_user = os.environ.get("DB_USER")
    db_pass = os.environ.get("DB_PASS")
    db_name = os.environ.get("DB_NAME")

    connector = Connector()

    def getconn():
        return connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=IPTypes.PUBLIC  # Uses Google's secure IAM tunnel
        )

    # Create a connection pool to reuse database connections
    return sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_size=1,
        max_overflow=1,
    )

db_pool = init_connection_pool()


@app.route("/")
def hello():
    return "Petwell Service Api!"


@app.route("/api/get-signed-url", methods=["POST"])
def get_signed_url():
    try:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Unauthorized: Missing or invalid token"}), 401

        # NOTE: token currently unused; keeping line in case you validate it later.
        _token = auth_header.split("Bearer ")[1]

        data = request.get_json(silent=True) or {}
        pet_id = data.get("petId")
        file_name = data.get("fileName")
        content_type = data.get("contentType")

        logger.info(
            "***PetId=%s FileName=%s ContentType=%s",
            pet_id,
            file_name,
            content_type,
        )

        if not all([pet_id, file_name, content_type]):
            return (
                jsonify({"error": "Missing required fields (petId, fileName, contentType)"}),
                400,
            )

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        blob_path = f"medical_records/{pet_id}/{file_name}"
        blob = bucket.blob(blob_path)

        credentials = get_credentials()

        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            method="PUT",
            content_type=content_type,
            service_account_email=credentials.service_account_email,
            access_token=credentials.token,
        )

        logger.info("***generated url: %s", url)

        print ("debug: before db insert")
         # --- STEP B: SAVE TO POSTGRES ---
            # Extract pet_id from the file path (e.g., 'medical_records/pet_1/RyanPhoto.jpg' -> 'pet_1')
           # pet_id = file_name.split("/")[1]
           
        with db_pool.connect() as db_conn:
            # Prepare the SQL Insert
            insert_stmt = sqlalchemy.text("""
                INSERT INTO medical_records (id, pet_id, blob_path, status)
                VALUES ("record1", "Gup1", "xxxpath", "new")
            """)
           
            # Execute it safely using parameterized variables
            db_conn.execute(insert_stmt, parameters={
                "id": "record1",
                "pet_id": "Gup1",
                "blob_path": "xxxpath",
                "status": "new" # Postgres loves JSON strings for lists
            })
            db_conn.commit()
           
        print("Successfully saved AI summary to Cloud SQL!")
        
        return jsonify({"signedUrl": url, "gcsFilePath": blob_path}), 200

    except Exception as e:
        logger.exception("Error generating signed URL: %s", e)
        return jsonify({"error": "Internal server error"}), 500
