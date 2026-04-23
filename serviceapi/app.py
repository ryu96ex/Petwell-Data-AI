import os
import datetime
import logging
import uuid
from typing import Optional

from fastapi import FastAPI, Header, HTTPException,Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi import Depends, Query
from pydantic import BaseModel

from google.cloud import storage

#fire base auth needed for user identity authentication
import firebase_admin
from firebase_admin import auth, credentials

#google auth needed for google cloud service account authentication
import google.auth
import google.auth.transport.requests

import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes

import logging

firebase_admin.initialize_app()

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
    allow_origins=["http://localhost:8081"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BUCKET_NAME = os.environ["BUCKET_NAME"]

_cached_credentials = None

def verify_firebase_uid(authorization: Optional[str] = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized: Missing or invalid token")

    token = authorization.split("Bearer ", 1)[1].strip()

    try:
        decoded_token = auth.verify_id_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    uid = decoded_token.get("uid")
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid token: uid missing")

    return uid

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
    petName: str
    fileName: str
    contentType: str


@app.get("/")
def hello():
    return "Petwell Service Api!"


@app.post("/api/get-signed-url")
def get_signed_url(payload: SignedUrlRequest, authorization: Optional[str] = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized: Missing or invalid token")

    token = authorization.split("Bearer ", 1)[1].strip()

    try:
        decoded_token = auth.verify_id_token(token)
        uid = decoded_token.get("uid")
        email = decoded_token.get("email")
        if not uid:
            raise HTTPException(status_code=401, detail="Invalid token: uid missing")
        logger.info("***Uid=%s Email=%s", uid, email)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Token verification failed: %s", e)
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    pet_name = payload.petName
    file_name = payload.fileName
    content_type = payload.contentType

    # Generate medical record UUID object 
    record_id = uuid.uuid4()

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Include UUID in blob path (as string)
    blob_path = f"medical_records/{uid}/{pet_name}/{str(record_id)}/{file_name}"
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

    # Create/ensure DB rows and track the record
    insert_meta_data(
        uid,
        email,
        pet_name,
        record_id=record_id,
        blob_path=blob_path,
    )

    return {
        "signedUrl": url,
        "gcsFilePath": blob_path,
        "recordId": str(record_id),  # JSON-friendly
    }

def get_or_create_app_user_id(db_conn, uid: Optional[str], email: Optional[str]):
    """
    Resolve (or create) app_user.id using firebase_uid when available, otherwise email.
    Returns: UUID (as returned by the driver).
    """
    if not uid and not email:
        raise ValueError("Either firebase_uid or email is required to identify the owner")

    if uid:
        user_row = db_conn.execute(
            sqlalchemy.text(
                """
                SELECT id
                FROM app_user
                WHERE firebase_uid = :firebase_uid
                LIMIT 1
                """
            ),
            {"firebase_uid": uid},
        ).fetchone()
    else:
        user_row = db_conn.execute(
            sqlalchemy.text(
                """
                SELECT id
                FROM app_user
                WHERE email = :email
                LIMIT 1
                """
            ),
            {"email": email},
        ).fetchone()

    if user_row:
        return user_row[0]

    # Create user (store both uid/email if provided)
    return db_conn.execute(
        sqlalchemy.text(
            """
            INSERT INTO app_user (id, firebase_uid, email, created_at)
            VALUES (gen_random_uuid(), :uid, :email, NOW())
            RETURNING id
            """
        ),
        {"uid": uid, "email": email},
    ).fetchone()[0]


def get_or_create_pet_id(db_conn, user_id, petName: str):
    """
    Resolve (or create) pets.id for a given user_id + pet name.
    Returns: UUID (as returned by the driver).
    """
    if not petName:
        raise ValueError("petName is required")

    pet_row = db_conn.execute(
        sqlalchemy.text(
            """
            SELECT id
            FROM pets
            WHERE user_id = :user_id AND name = :name
            LIMIT 1
            """
        ),
        {"user_id": user_id, "name": petName},
    ).fetchone()

    if pet_row:
        return pet_row[0]

    return db_conn.execute(
        sqlalchemy.text(
            """
            INSERT INTO pets (user_id, name)
            VALUES (:user_id, :name)
            RETURNING id
            """
        ),
        {"user_id": user_id, "name": petName},
    ).fetchone()[0]

def create_medical_record_row(
    db_conn,
    *,
    record_id: uuid.UUID,
    pet_id,
    blob_path: str,
    status: str = "UPLOADING",
):
    """
    Insert a new medical_records row.
    medical_records schema:
      id (uuid PK), pet_id (uuid), blob_path (text unique), status (text),
      created_at/updated_at (timestamptz), visit_date (date null), error (text null)
    """
    db_conn.execute(
        sqlalchemy.text(
            """
            INSERT INTO medical_records (id, pet_id, blob_path, status, created_at, updated_at)
            VALUES (:id, :pet_id, :blob_path, :status, NOW(), NOW())
            """
        ),
        {
            "id": record_id,        # uuid.UUID is fine for Postgres UUID columns
            "pet_id": pet_id,
            "blob_path": blob_path, # text
            "status": status,
        },
    )

def insert_meta_data(
    uid: str,
    email: str,
    petName: str,
    *,
    record_id: uuid.UUID,
    blob_path: str,
):
    """
    Ensures app_user + pet exist, then creates a medical_records row tied to blob_path.
    """
    try:
        with db_pool.connect() as db_conn:
            user_id = get_or_create_app_user_id(db_conn, uid=uid, email=email)
            pet_id = get_or_create_pet_id(db_conn, user_id=user_id, petName=petName)

            create_medical_record_row(
                db_conn,
                record_id=record_id,
                pet_id=pet_id,
                blob_path=blob_path,
                status="UPLOADING",
            )

            db_conn.commit()

        return {
            "user_id": str(user_id),
            "pet_id": str(pet_id),
            "medical_record_id": str(record_id),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("DB insert failed (user/pet/medical_record): %s", e)
        raise HTTPException(status_code=500, detail="DB insert failed")

@app.get("/api/get-pet-trends")
def get_pet_trends(
    petName: str = Query(...), #required Query Parameter being passed in through API call
    metric: str = Query("ALT"), #using default metric value of ALT if caller doesn't specify
    uid: str = Depends(verify_firebase_uid),
):
    try:
        with db_pool.connect() as conn:
            query = sqlalchemy.text("""
                SELECT lr.measured_date, lr.value_num
                FROM lab_results lr
                JOIN medical_records mr ON lr.record_id = mr.id
                JOIN pets p ON mr.pet_id = p.id
                JOIN app_user u ON p.user_id = u.id
                WHERE p.name = :pet_name
                  AND u.firebase_uid = :firebase_uid
                  AND lr.metric_code = :metric
                ORDER BY lr.measured_date ASC
            """)

            rows = conn.execute(query, {
                "pet_name": petName,
                "metric": metric,
                "firebase_uid": uid,
            }).fetchall()

        trends = [
            {
                "value": float(row[1]),
                "label": row[0].strftime("%b %d %Y") if isinstance(row[0], datetime.date) else str(row[0]),
            }
            for row in rows
        ]

        return {"petName": petName, "metric": metric, "trends": trends, "verified_uid": uid}

    except Exception as e:
        logger.exception("DB Fetch Error: %s", e)
        raise HTTPException(status_code=500, detail="Could not fetch medical trends")
