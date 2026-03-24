import base64
import json
import logging
import os
import re
import hashlib
import io
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from google.cloud import tasks_v2
from google.cloud import storage
from google.cloud import vision
from google.api_core import exceptions as gcp_exceptions
from google.cloud.sql.connector import Connector, IPTypes
from pypdf import PdfReader

import sqlalchemy


import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.exception_handler(HTTPException)
async def log_http_exception(request: Request, exc: HTTPException):
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

def if_pdf_path(blob_path: str) -> bool:
    # worker only handles pdf uploads
    return blob_path.endswith(".pdf")

def enqueue_task(*, bucket: str, blob_path: str, generation: Optional[str], pubsub_message_id: Optional[str]) -> str:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    location = os.environ["TASKS_LOCATION"]
    queue = os.environ["TASKS_QUEUE"]
    worker_base_url = os.environ["WORKER_BASE_URL"]
    invoker_sa = os.environ.get("TASKS_INVOKER_SA")  # optional but recommended

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)

    task_payload = {
        "bucket": bucket,
        "blob_path": blob_path,
        "generation": generation,
        "pubsub_message_id": pubsub_message_id,
    }

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{worker_base_url}/tasks/process",
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(task_payload).encode("utf-8"),
        }
    }

    if invoker_sa:
        task["http_request"]["oidc_token"] = {
            "service_account_email": invoker_sa,
            "audience": worker_base_url,
        }

    # Idempotent enqueue: deterministic task name
    # Build a job key string using bucket + name + generation and hash it to a short safe ID:

    job_key = f"{bucket}/{blob_path}#{generation or ''}"
    task_hash = hashlib.sha256(job_key.encode("utf-8")).hexdigest()[:32]
    task_id = f"process-{task_hash}"
    task["name"] = client.task_path(project, location, queue, task_id)
    
    created = client.create_task(request={"parent": parent, "task": task})
    return created.name


@app.get("/")
def hello():
    return "Hello from Petwell!"


@app.post("/pubsub/push")
async def pubsub_push(request: Request):
    envelope = await request.json()
    if not envelope or "message" not in envelope:
        raise HTTPException(status_code=400, detail="Invalid Pub/Sub envelope")

    msg = envelope["message"]
    data_b64 = msg.get("data")
    if not data_b64:
        raise HTTPException(status_code=400, detail="Missing message.data")

    try:
        payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Bad data: {e}")

    bucket = payload.get("bucket") or payload.get("bucketId")
    blob_path = payload.get("name") or payload.get("objectId")
    generation = payload.get("generation")
    message_id = msg.get("messageId")

    if not bucket or not blob_path:
        raise HTTPException(status_code=400, detail=f"Missing bucket/name in payload: {payload}")

    logger.info("Received: gs://%s/%s gen=%s messageId=%s", bucket, blob_path, generation, message_id)

    try:
        task_name = enqueue_task(
            bucket=bucket,
            blob_path=blob_path,
            generation=generation,
            pubsub_message_id=message_id,
        )
        logger.info("Enqueued task: %s", task_name)
    except Exception as e:
        logger.exception("Failed to enqueue Cloud Task: %s", e)
        raise HTTPException(status_code=500, detail="Failed to enqueue Cloud Task")

    # Pub/Sub considers any 2xx an ack
    return {}


@app.post("/tasks/process")
async def tasks_process(payload: dict):
    bucket = payload.get("bucket")
    blob_path = payload.get("blob_path")
    if not bucket or not blob_path:
        raise HTTPException(status_code=400, detail=f"Missing bucket/blob_path: {payload}")

    # TODO: implement OCR + Vertex + DB updates

    # Ignore non-PDF files routed from the same storage notification pipeline.
    if not _is_pdf_path(blob_path):
        logger.info("Skipping non-PDF object: gs://%s/%s", bucket, blob_path)
        return {"status": "skipped", "reason": "non_pdf"}

    logger.info("Processing PDF: gs://%s/%s gen=%s", bucket, blob_path, generation)

    return {}
