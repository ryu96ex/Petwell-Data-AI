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


# db_pool = init_connection_pool()

db_pool = None

if all([
    os.environ.get("DB_INSTANCE_CONNECTION_NAME"),
    os.environ.get("DB_USER"),
    os.environ.get("DB_PASS"),
    os.environ.get("DB_NAME"),
]):
    db_pool = init_connection_pool()
else:
    logger.warning("Database not configured; starting without db_pool.")


def if_pdf_path(blob_path: str) -> bool:
    # worker only handles pdf uploads
    return blob_path.endswith(".pdf")

def _extract_text_with_pdf_reader(
    *,
    bucket: str,
    blob_path: str,
    generation: Optional[str] = None,
) -> str:
    # First-pass extraction for text-based PDFs (fast, no OCR cost).
    storage_client = storage.Client()
    blob = storage_client.bucket(bucket).blob(blob_path, generation=generation)
    file_bytes = blob.download_as_bytes()

    reader = PdfReader(io.BytesIO(file_bytes))
    page_text_chunks = []
    for page in reader.pages:
        page_text_chunks.append(page.extract_text() or "")
    return "\n".join(page_text_chunks).strip()

def _needs_ocr(text: str) -> bool:
    compact = re.sub(r"\s+", "", text or "")
    # Heuristic: scanned/image PDFs typically return near empty text from pypdf.
    return len(compact) < 100

def _extract_text_with_vision_ocr(*, bucket: str, blob_path: str, task_id: str) -> str:
    # Fallback path for scanned/image PDFs using Vision async file OCR.
    vision_client = vision.ImageAnnotatorClient()
    storage_client = storage.Client()

    input_uri = f"gs://{bucket}/{blob_path}"
    output_bucket = os.environ.get("OCR_OUTPUT_BUCKET", bucket)
    output_prefix = os.environ.get("OCR_OUTPUT_PREFIX", "ocr-output").strip("/")
    # Use task scoped prefix so OCR output files are isolated per document
    destination_uri = f"gs://{output_bucket}/{output_prefix}/{task_id}/"

    request = vision.AsyncAnnotateFileRequest(
        features=[vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)],
        input_config=vision.InputConfig(gcs_source=vision.GcsSource(uri=input_uri), mime_type="application/pdf"),
        output_config=vision.OutputConfig(
            gcs_destination=vision.GcsDestination(uri=destination_uri),
            batch_size=5,
        ),
    )

    operation = vision_client.async_batch_annotate_files(requests=[request])
    # Wait for asynchronous OCR to complete before collecting output JSON files
    operation.result(timeout=600)

    destination_path = destination_uri.replace("gs://", "", 1)
    output_bucket_name, _, prefix = destination_path.partition("/")

    ocr_text_parts = []
    for result_blob in storage_client.list_blobs(output_bucket_name, prefix=prefix):
        if not result_blob.name.endswith(".json"):
            continue
        # Vision writes one or more JSON shards
        # Concatenate text from all responses
        payload = json.loads(result_blob.download_as_bytes())
        for response_group in payload.get("responses", []):
            full_text = response_group.get("fullTextAnnotation", {}).get("text", "")
            if full_text:
                ocr_text_parts.append(full_text)

    return "\n".join(ocr_text_parts).strip()

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
    generation = payload.get("generation")
    pubsub_message_id = payload.get("pubsub_message_id") or "unknown"
    if not bucket or not blob_path:
        raise HTTPException(status_code=400, detail=f"Missing bucket/blob_path: {payload}")

    # TODO: implement OCR + Vertex + DB updates

    # Ignore non-PDF files routed from the same storage notification pipeline.
    if not if_pdf_path(blob_path):
        logger.info("Skipping non-PDF object: gs://%s/%s", bucket, blob_path)
        return {"status": "skipped", "reason": "non_pdf"}

    logger.info("Processing PDF: gs://%s/%s gen=%s", bucket, blob_path, generation)

    try:
        # Attempt parser-based extraction first; OCR only when text is insufficient.
        parsed_text = _extract_text_with_pdf_reader(bucket=bucket, blob_path=blob_path, generation=generation)
        extraction_mode = "pdf_parser"

        # Run OCR if needed
        if _needs_ocr(parsed_text):
            logger.info("PDF parser yielded insufficient text; running OCR for gs://%s/%s", bucket, blob_path,)
            task_id = hashlib.sha256(f"{bucket}/{blob_path}#{generation or ''}".encode("utf-8")).hexdigest()[:32]
            parsed_text = _extract_text_with_vision_ocr(bucket=bucket, blob_path=blob_path, task_id=task_id)
            extraction_mode = "ocr"
        
        # Fail explicitly when both parser and OCR produce no usable text
        if not parsed_text:
            raise HTTPException(status_code=422, detail="Failed to extract text from PDF")

        logger.info(
            "Text extraction complete mode=%s chars=%s messageId=%s",
            extraction_mode,
            parsed_text,
            len(parsed_text),
            pubsub_message_id,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("PDF processing failed for gs://%s/%s: %s", bucket, blob_path, e)
        raise HTTPException(status_code=500, detail="PDF processing failed")

    # TODO: implement AI JSON structuring + DB updates with extracted text.

    #return {"status": "processed", "extraction_mode": extraction_mode, "text_length": len(parsed_text)}
    # return a success message and 200 status 
    return JSONResponse(status_code=200, content={"processed"})
