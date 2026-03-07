
import os
import datetime
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import storage

# ADD these imports
import google.auth
from google.auth import iam
from google.auth.transport.requests import Request

credentials, project_id = google.auth.default()
# CRITICAL NEW STEP: Refresh the credentials to actually generate the token
auth_request = google.auth.transport.requests.Request()
credentials.refresh(auth_request)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

CORS(app, origins=["http://localhost:5173"])

@app.route('/')
def hello():
    return 'Petwell Service Api!'

BUCKET_NAME = os.environ["BUCKET_NAME"]

@app.route("api/get-signed-url", methods=["POST"])
def get_signed_url():
    try:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Unauthorized: Missing or invalid token"}), 401

        token = auth_header.split("Bearer ")[1]

        data = request.get_json()
        pet_id = data.get("petId")
        file_name = data.get("fileName")
        content_type = data.get("contentType")

        logger.info("***PetId=%s FileName=%s ContentType=%s", pet_id, file_name, content_type)

        if not all([pet_id, file_name, content_type]):
            return jsonify({"error": "Missing required fields (petId, fileName, contentType)"}), 400

        logger.info("***Passed required field check")
       
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        blob_path = f"medical_records/{pet_id}/{file_name}"
        blob = bucket.blob(blob_path)

        # NEW: use IAM signer (no private key file needed)
            
        #signer = iam.Signer(Request(), credentials, credentials.service_account_email)

        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            method="PUT",
            content_type=content_type,
            service_account_email=credentials.service_account_email,
            access_token=credentials.token
        )

        logger.info("***generated url: ", url)
        #return url
        return jsonify({"signedUrl": url, "gcsFilePath": blob_path}), 200

    except Exception:
        logger.exception("Error generating signed URL: %s", e)  
        return jsonify({"error": "Internal server error"}), 500
