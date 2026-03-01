import os
import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import storage

app = Flask(__name__)

CORS(app, origins=["http://localhost:5173"])

@app.route('/')
def hello():
    return 'Hello from Petwell!'

@app.route("/get-signed-url", methods=["POST"])
def get_signed_url():
    try:
        # Step 1: Check for the Firebase Auth Token sent by React
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Unauthorized: Missing or invalid token"}), 401
       
        # Note: In a production environment, you would use the `firebase_admin`
        # Python SDK right here to verify the token and extract the user's UID.
        token = auth_header.split("Bearer ")[1]
       
        # Step 2: Parse the incoming JSON from React
        data = request.get_json()
        pet_id = data.get("petId")
        file_name = data.get("fileName")
        content_type = data.get("contentType")


        if not all([pet_id, file_name, content_type]):
            return jsonify({"error": "Missing required fields (petId, fileName, contentType)"}), 400


        # Step 3: Connect to Google Cloud Storage
        # This will automatically use the permissions of the Cloud Run service account
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
       
        # Create a clean, organized path in the bucket (e.g., medical_records/pet_1/results.pdf)
        blob_path = f"medical_records/{pet_id}/{file_name}"
        blob = bucket.blob(blob_path)


        # Step 4: Generate the v4 Signed URL for a PUT request
        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(minutes=15),  # URL expires in 15 mins
            method="PUT",
            content_type=content_type,
        )


        # Step 5: Send the URL back to React
        return jsonify({
            "signedUrl": url,
            "gcsFilePath": blob_path
        }), 200


    except Exception as e:
        print(f"Error generating signed URL: {e}")
        return jsonify({"error": "Internal server error"}), 500

