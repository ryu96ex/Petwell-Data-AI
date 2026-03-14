import os
import datetime
import logging
from typing import Optional

from fastapi import FastAPI, Header, HTTPException,Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from google.cloud import storage

import google.auth
import google.auth.transport.requests

import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.exception_handler(HTTPException)
async def log_http_exception(request: Request, exc: HTTPException):
    # 4xx are expected client errors; 5xx are server errors
    if 400 <= exc.status_code < 500:
        logger.warning(
            "HTTPException %s %s -> %s (%s)",
            request.method,
            request.url.path,
            exc.status_code,
            exc.detail,
        )
    else:
        logger.error(
            "HTTPException %s %s -> %s (%s)",
            request.method,
            request.url.path,
            exc.status_code,
            exc.detail,
        )
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def log_unhandled_exception(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


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
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized: Missing or invalid token")

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

    # Currently inserts a hard-coded user
    # Replace this with real inserts later.
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

        raise HTTPException(status_code=500, detail="DB insert failed")

    return {"signedUrl": url, "gcsFilePath": blob_path}
