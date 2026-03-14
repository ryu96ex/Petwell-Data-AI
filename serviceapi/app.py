import os
import datetime
import logging
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from google.cloud import storage

import google.auth
import google.auth.transport.requests

import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# CORS (same behavior as your Flask version)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BUCKET_NAME = os.environ["BUCKET_NAME"]

_cached_credentials = None


def get_credentials():
    """Refresh ADC credentials and cache them (used for signed URL token signing)."""
    global _cached_credentials
    if _cached_credentials is None or not _cached_credentials.valid:
        credentials, _project_id = google.auth.default()
        auth_request = google.auth.transport.requests.Request()
        credentials.refresh(auth_request)
        _cached_credentials = credentials
    return _cached_credentials


def init_connection_pool() -> sqlalchemy.engine.Engine:
    instance_connection_name = os.environ.get("DB_INSTANCE_CONNECTION_NAME")
    db_user = os.environ.get("DB_USER")
    db_pass = os.environ.get("DB_PASS")
    db_name = os.environ.get("DB_NAME")

    if not all([instance_connection_name, db_user, db_pass, db_name]):
        logger.warning("DB env vars missing; db_pool will still init but may fail at runtime.")

    connector = Connector()

    def getconn():
        return connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=IPTypes.PUBLIC,
        )

    return sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_size=1,
        max_overflow=1,
    )


db_pool = init_connection_pool()


class SignedUrlRequest(BaseModel):
    petId: str
    fileName: str
    contentType: str


@app.get("/")
def hello():
    return "Petwell Service Api!"


@app.post("/api/get-signed-url")
def get_signed_url(payload: SignedUrlRequest, authorization: Optional[str] = Header(default=None)):
    # Step 1: Auth header presence (you can replace with Firebase verification later)
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized: Missing or invalid token")

    # NOTE: token currently unused; keeping in case you validate it later.
    _token = authorization.split("Bearer ")[1]

    pet_id = payload.petId
    file_name = payload.fileName
    content_type = payload.contentType

    logger.info("***PetId=%s FileName=%s ContentType=%s", pet_id, file_name, content_type)

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
        service_account_email=getattr(credentials, "service_account_email", None),
        access_token=credentials.token,
    )

    # Your DB insert block currently inserts a hard-coded user; preserving behavior but fixing indentation.
    # You will likely replace this with real inserts later.
    try:
        with db_pool.connect() as db_conn:
            insert_stmt = sqlalchemy.text(
                """
                INSERT INTO app_user (id, firebase_uid, email, created_at)
                VALUES (gen_random_uuid(), 'ryanyu', 'ryandyu@gmail.com', NOW())
                """
            )
            db_conn.execute(insert_stmt)
            db_conn.commit()
    except Exception as e:
        logger.exception("DB insert failed: %s", e)
        # Decide if you want to fail the request or proceed. For now, fail loudly:
        raise HTTPException(status_code=500, detail="DB insert failed")

    return {"signedUrl": url, "gcsFilePath": blob_path}
