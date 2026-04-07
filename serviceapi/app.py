import os
import datetime
import logging
from typing import Optional

from flask import Flask, request, jsonify
from fastapi import FastAPI, Header, HTTPException,Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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

class UserPetRequest(BaseModel):
    userId: str
    petId: str

class CreateUserPetRequest(BaseModel):
    userId: str
    petName: str
    petId: str

class HealthRecordRequest(BaseModel): 
    userId: str
    petId: str
    filepath: str
    filename: str
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
                VALUES (gen_random_uuid(), :uid, :email, NOW())
                ON CONFLICT (firebase_uid) DO NOTHING
                """
            )
            db_conn.execute(insert_stmt, {
                "uid": uid,
                "email": email
            })
            db_conn.commit()
    except Exception as e:
        logger.exception("DB insert failed: %s", e)        
        raise HTTPException(status_code=500, detail="DB insert failed")

    return {"signedUrl": url, "gcsFilePath": blob_path}



#get a user's ID and pet's ID from the request, validate the user exists in the database, and return a success message if both are valid.
@app.post("/api/get-user-and-pet-ID")
def get_user_and_pet(payload: UserPetRequest):
    try:
        user_id = payload.userId
        pet_id = payload.petId

        logger.info("Received userId=%s, petId=%s", user_id, pet_id)

        with db_pool.connect() as conn:

            #Checks if user already exists
            user_query = sqlalchemy.text("""
                SELECT 1 FROM app_user
                WHERE firebase_uid = :user_id
                LIMIT 1
            """)
            user_result = conn.execute(user_query, {"user_id": user_id}).fetchone()

            if not user_result:
                raise HTTPException(status_code=404, detail="User not found")

            #Check pet exists (if you have a pets table)
            pet_query = sqlalchemy.text("""
                SELECT 1 FROM pet
                WHERE id = :pet_id AND user_id = :user_id
                LIMIT 1
            """)
            pet_result = conn.execute(pet_query, {
                "pet_id": pet_id,
                "user_id": user_id
            }).fetchone()

            if not pet_result:
                raise HTTPException(status_code=404, detail="Pet not found")

        return {
            "message": "User and Pet validated successfully",
            "userId": user_id,
            "petId": pet_id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error validating user and pet: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")
    

#creates a user and pet record in the database if they don't already exist
@app.post("/api/create-user-and-pet")
def create_user_and_pet(payload: CreateUserPetRequest):
    try:
        user_id = payload.userId
        email = payload.email
        pet_id = payload.petId

        with db_pool.connect() as conn:
            # Insert user if not exists
            insert_user = sqlalchemy.text("""
                INSERT INTO app_user (id, firebase_uid, email, created_at)
                VALUES (gen_random_uuid(), :user_id, :email, NOW())
                ON CONFLICT (firebase_uid) DO NOTHING
            """)

            conn.execute(insert_user, {
                "user_id": user_id,
                "email": email
            })

            # Insert pet if not exists
            insert_pet = sqlalchemy.text("""
                INSERT INTO pet (id, user_id, created_at)
                VALUES (:pet_id, :user_id, NOW())
                ON CONFLICT (id) DO NOTHING
            """)

            conn.execute(insert_pet, {
                "pet_id": pet_id,
                "user_id": user_id
            })

            conn.commit()

        return {
            "message": "User and Pet created successfully",
            "userId": user_id,
            "petId": pet_id
        }

    except Exception as e:
        logger.exception("Error creating user and pet: %s", e)
        raise HTTPException(status_code=500, detail="Failed to create user and pet")
    

@app.post("/api/upload-health-record")
def upload_health_record(payload: HealthRecordRequest):
    try:
        user_id = payload.userId
        pet_id = payload.petId
        filepath = payload.filepath
        filename = payload.filename
        content_type = payload.contentType

        # Here you would add logic to validate the user and pet, and then process the health record upload.
        # This is a placeholder for demonstration purposes.

        return {
            "message": "Health record uploaded successfully",
            "userId": user_id,
            "petId": pet_id,
            "filePath": filepath,
            "fileName": filename,
            "contentType": content_type
        }

    except Exception as e:
        logger.exception("Error uploading health record: %s", e)
        raise HTTPException(status_code=500, detail="Failed to upload health record")