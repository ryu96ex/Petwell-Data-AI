import os
import datetime
import logging
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

    token = authorization.split("Bearer ")[1]

    try:
        decoded_token = auth.verify_id_token(token)
        uid = decoded_token.get('uid')
        email = decoded_token.get('email')
        logger.info("***Uid=%s Email%s", uid, email)
    except Exception as e:
        logger.error("Token verification failed: %s", e)
        return JSONResponse({"error": "Invalid token"}), 401
    
    
    pet_name = payload.petName
    file_name = payload.fileName
    content_type = payload.contentType

    logger.info("***PetName=%s FileName=%s ContentType=%s", pet_name, file_name, content_type)

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    blob_path = f"medical_records/{uid}/{pet_name}/{file_name}"
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
    # try:
    #     with db_pool.connect() as db_conn:
            
    #         insert_stmt = sqlalchemy.text(
    #             """
    #             INSERT INTO app_user (id, firebase_uid, email, created_at)
    #             VALUES (gen_random_uuid(), :uid, :email, NOW())
    #             ON CONFLICT (firebase_uid) DO NOTHING
    #             """
    #         )
    #         db_conn.execute(insert_stmt, {
    #             "uid": uid,
    #             "email": email
    #         })
    #         db_conn.commit()
    # except Exception as e:
    #     logger.exception("DB insert failed: %s", e)        
    #     raise HTTPException(status_code=500, detail="DB insert failed")

    #check for existing owner and insert new record if they do not exist
    insert_meta_data(uid, email, pet_name)

    return {"signedUrl": url, "gcsFilePath": blob_path}

def insert_meta_data(uid: str, email: str, petName: str):

    #use test value for email until the client app is enabled to pass email from firebase auth

    email = "ry96@njit.edu"
    
    if not uid and not email:
        raise ValueError("Either firebase_uid or email is required to identify the owner")

    try:
        with db_pool.connect() as db_conn:
            #Check if firebase user id or email already exists in the app_user table
            if uid: 
                select_stmt = sqlalchemy.text(
                    """
                    SELECT id
                    from app_user
                    where firebase_uid = :firebase_uid
                    LIMIT 1
                    """
                )
                result = db_conn.execute(select_stmt, {
                    "firebase_uid" : uid
                })
            else:
                select_stmt = sqlalchemy.text(
                    """
                    SELECT id
                    from app_user
                    where email = :email
                    LIMIT 1
                    """
                )
                result = db_conn.execute(select_stmt, {
                    "email" : email
                })

            row = result.fetchone()

            #Insert new owner if it doesn't already exist in the app_user table
            if row:
                user_id  = row[0]
            else:  
                insert_stmt = sqlalchemy.text(
                    """
                    INSERT INTO app_user (id, firebase_uid, email, created_at)
                    VALUES (gen_random_uuid(), :uid, :email, NOW())
                    RETURNING ID
                    ON CONFLICT (firebase_uid) DO NOTHING
                    """
                )
                result = db_conn.execute(insert_stmt, {
                    "uid": uid,
                    "email": email
                })
                
                user_id = result.fetchone()[0]

            #Check if pet with corresponding owner's user_id exists
            
            select_stmt = sqlalchemy.text(
                """
                SELECT id
                FROM pets
                WHERE user_id = :user_id AND name = :name
                LIMIT 1
                """
            )
            
            result = db_conn.execute(select_stmt, {
                "user_id": user_id,
                "name": petName
            })

            row = result.fetchone()[0]

            #Insert new pet with corresponding owner's user id into pets table
            if row:
                pet_id = row[0]
            else:
                insert_stmt = sqlalchemy.text(
                    """
                    INSERT INTO pets (user_id, name)
                    VALUES(:user_id,:name)
                    RETURNING ID
                    """
                )    
                result = db_conn.execute(insert_stmt, {
                    "user_id": user_id,
                    "name": petName
                })

            pet_id = result.fetchone()[0]
            
            db_conn.commit()
                
    except Exception as e:
        logger.exception("DB insert for metadata failed: %s", e)        
        raise HTTPException(status_code=500, detail="DB insert for metadata failed")

    return {"user_id": str(user_id), "pet_id:": str(pet_id)}

@app.get("/api/get-pet-trends")
def get_pet_trends(
    petName: str = Query(...),
    metric: str = Query("ALT"),
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
                "label": row[0].strftime("%b %d") if isinstance(row[0], datetime.date) else str(row[0]),
            }
            for row in rows
        ]

        return {"petName": petName, "metric": metric, "trends": trends, "verified_uid": uid}

    except Exception as e:
        logger.exception("DB Fetch Error: %s", e)
        raise HTTPException(status_code=500, detail="Could not fetch medical trends")


