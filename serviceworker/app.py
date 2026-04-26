import base64
import json
import logging
import os
import re
import hashlib
import io
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from google.cloud import tasks_v2
from google.cloud import storage
from google.cloud import vision
from google.api_core import exceptions as gcp_exceptions
from google.cloud.sql.connector import Connector, IPTypes
from pypdf import PdfReader
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

import sqlalchemy
from sqlalchemy import text

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

_vertex_model: Optional[GenerativeModel] = None

def _is_pdf_path(blob_path: str) -> bool:
    # worker only handles pdf uploads
    return blob_path.lower().endswith(".pdf")


def _is_supported_image_path(blob_path: str) -> bool:
    return blob_path.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))

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


def _extract_text_from_image_with_vision(
    *,
    bucket: str,
    blob_path: str,
    generation: Optional[str] = None,
) -> str:
    # OCR image bytes directly for jpg/png uploads.
    storage_client = storage.Client()
    vision_client = vision.ImageAnnotatorClient()
    blob = storage_client.bucket(bucket).blob(blob_path, generation=generation)
    image_bytes = blob.download_as_bytes()

    response = vision_client.document_text_detection(image=vision.Image(content=image_bytes))
    if response.error.message:
        raise RuntimeError(f"Vision OCR error: {response.error.message}")
    return (response.full_text_annotation.text or "").strip()


def _extract_json_object(raw: str) -> Any:
    trimmed = (raw or "").strip()
    if trimmed.startswith("```"):
        trimmed = re.sub(r"^```(?:json)?\s*", "", trimmed)
        trimmed = re.sub(r"\s*```$", "", trimmed)

    try:
        return json.loads(trimmed)
    except json.JSONDecodeError as e:
        logger.error("Model returned invalid JSON: %s; first_500=%r", e, trimmed[:500])
        raise HTTPException(status_code=502, detail="Vertex returned invalid JSON")


def _parse_iso_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # Accept "YYYY-MM-DD" or full ISO timestamps by taking the date prefix.
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")


def _parse_measurement_value(raw: str) -> tuple[Optional[Decimal], Optional[str]]:
    s = (raw or "").strip()
    if not s:
        return None, None

    m = _NUM_RE.search(s)
    if not m:
        return None, s

    num_token = m.group(0)
    try:
        num = Decimal(num_token)
    except InvalidOperation:
        return None, s

    prefix = s[: m.start()].strip()
    suffix = s[m.end() :].strip()
    if prefix or suffix:
        # Values like "<13.5" or "13.5 (H)" should remain human-readable in value_text.
        return num, s

    return num, None


_TRAILING_PAREN_RE = re.compile(r"\s*\(.*\)\s*$")

# Maps normalized base tokens to their canonical metric code.
_METRIC_ALIASES: dict[str, str] = {
    "SGPT": "ALT",
    "ALT/SGPT": "ALT",
}


def normalize_metric_code(name: str) -> str:
    """Return a canonical metric key for *name*.

    Rules applied in order:
    1. Remove a trailing parenthetical expression, e.g. "ALT (SGPT)" -> "ALT".
    2. Strip surrounding whitespace and normalize internal runs of whitespace.
    3. Uppercase the result.
    4. Apply the alias map for well-known synonyms, e.g. "SGPT" -> "ALT".
    """
    s = _TRAILING_PAREN_RE.sub("", name).strip()  # drop trailing parenthetical first
    s = " ".join(s.split())                        # normalize remaining whitespace
    s = s.upper()
    return _METRIC_ALIASES.get(s, s)


def _normalize_blob_path_candidates(blob_path: str) -> list[str]:
    p = (blob_path or "").strip()
    if not p:
        return []

    candidates: list[str] = []
    for variant in {p, p.lstrip("/")}:
        if variant and variant not in candidates:
            candidates.append(variant)

    return candidates


def _mark_medical_record_processing(*, conn: sqlalchemy.Connection, blob_path: str) -> str:
    candidates = _normalize_blob_path_candidates(blob_path)
    if not candidates:
        raise RuntimeError("Empty blob_path")

    row = None
    for cand in candidates:
        row = conn.execute(
            text(
                """
                UPDATE public.medical_records
                SET status = 'PROCESSING',
                    error = NULL,
                    updated_at = NOW()
                WHERE blob_path = :p
                  AND status IN ('UPLOADING', 'PROCESSING', 'FAILED')
                RETURNING id::text AS id
                """
            ),
            {"p": cand},
        ).mappings().first()
        if row:
            break

        # Case-insensitive exact match fallback (helps if casing differs between GCS object name and DB text)
        row = conn.execute(
            text(
                """
                UPDATE public.medical_records
                SET status = 'PROCESSING',
                    error = NULL,
                    updated_at = NOW()
                WHERE blob_path ILIKE :p
                  AND status IN ('UPLOADING', 'PROCESSING', 'FAILED')
                RETURNING id::text AS id
                """
            ),
            {"p": cand},
        ).mappings().first()
        if row:
            break

    if not row:
        raise RuntimeError(f"No updatable medical_records row found for blob_path={blob_path!r}")

    return str(row["id"])


def _mark_medical_record_completed(
    *,
    conn: sqlalchemy.Connection,
    record_id: str,
    visit_date: Optional[date],
) -> None:
    conn.execute(
        text(
            """
            UPDATE public.medical_records
            SET status = 'COMPLETED',
                error = NULL,
                visit_date = COALESCE(:visit_date, visit_date),
                updated_at = NOW()
            WHERE id = CAST(:record_id AS uuid)
            """
        ),
        {"record_id": record_id, "visit_date": visit_date},
    )


def _mark_medical_record_failed(*, conn: sqlalchemy.Connection, record_id: str, message: str) -> None:
    # Keep message short; medical_records.error is text without a known max length, but logs/UX benefit from brevity.
    err = (message or "Processing failed").strip()
    if len(err) > 2000:
        err = err[:2000] + "…"

    conn.execute(
        text(
            """
            UPDATE public.medical_records
            SET status = 'FAILED',
                error = :error,
                updated_at = NOW()
            WHERE id = CAST(:record_id AS uuid)
            """
        ),
        {"record_id": record_id, "error": err},
    )


def _replace_lab_results(*, conn: sqlalchemy.Connection, record_id: str, structured: dict) -> int:
    visit_date = _parse_iso_date(structured.get("visit_date"))

    measurements = structured.get("measurements") or []
    if not isinstance(measurements, list):
        measurements = []

    conn.execute(
        text("DELETE FROM public.lab_results WHERE record_id = CAST(:record_id AS uuid)"),
        {"record_id": record_id},
    )

    rows: list[dict[str, Any]] = []
    for item in measurements:
        if not isinstance(item, dict):
            continue

        name = (item.get("name") or "").strip()
        if not name:
            continue

        value_raw = item.get("value")
        value_str = "" if value_raw is None else str(value_raw).strip()
        unit = item.get("unit")
        unit_str = None if unit is None else str(unit).strip()
        if unit_str == "":
            unit_str = None

        value_num, value_text = _parse_measurement_value(value_str)

        rows.append(
            {
                "record_id": record_id,
                "metric_code": name,
                "metric_canonical": normalize_metric_code(name),
                "measured_date": visit_date,
                "value_num": value_num,
                "value_text": value_text,
                "unit": unit_str,
                "reference_range": None,
            }
        )

    if not rows:
        return 0

    conn.execute(
        text(
            """
            INSERT INTO public.lab_results
                (record_id, metric_code, metric_canonical, measured_date, value_num, value_text, unit, reference_range)
            VALUES
                (CAST(:record_id AS uuid), :metric_code, :metric_canonical, :measured_date, :value_num, :value_text, :unit, :reference_range)
            """
        ),
        rows,
    )
    return len(rows)


def _persist_structured_data_to_db(*, blob_path: str, structured: dict) -> dict:
    if db_pool is None:
        raise RuntimeError("Database is not configured on this instance (missing db_pool).")

    visit_date = _parse_iso_date(structured.get("visit_date"))

    with db_pool.connect() as conn:
        with conn.begin():
            record_id = _mark_medical_record_processing(conn=conn, blob_path=blob_path)
            inserted = _replace_lab_results(conn=conn, record_id=record_id, structured=structured)
            _mark_medical_record_completed(conn=conn, record_id=record_id, visit_date=visit_date)

    return {"medical_record_id": record_id, "lab_results_inserted": inserted}


def _find_medical_record_id_by_blob_path(*, conn: sqlalchemy.Connection, blob_path: str) -> Optional[str]:
    candidates = _normalize_blob_path_candidates(blob_path)
    if not candidates:
        return None

    for cand in candidates:
        row = conn.execute(
            text(
                """
                SELECT id::text AS id
                FROM public.medical_records
                WHERE blob_path = :p
                LIMIT 1
                """
            ),
            {"p": cand},
        ).mappings().first()
        if row:
            return str(row["id"])

        row = conn.execute(
            text(
                """
                SELECT id::text AS id
                FROM public.medical_records
                WHERE blob_path ILIKE :p
                LIMIT 1
                """
            ),
            {"p": cand},
        ).mappings().first()
        if row:
            return str(row["id"])

    return None

def _get_vertex_model() -> GenerativeModel:
    global _vertex_model
    if _vertex_model is None:
        project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
        location = os.environ.get("VERTEX_LOCATION")
        model_name = os.environ.get("VERTEX_MODEL")
        if not project:
            raise RuntimeError("Missing GOOGLE_CLOUD_PROJECT or GCP_PROJECT for Vertex AI")
        vertexai.init(project=project, location=location)
        _vertex_model = GenerativeModel(model_name)
        logger.info("Vertex model initialized project=%s location=%s model=%s", project, location, model_name)
    return _vertex_model

def _extract_structured_data_with_vertex(raw_text: str) -> dict:

    model = _get_vertex_model()

    prompt = (
        "Extract structured medical-record data from the text and return JSON only.\n"
        "Required shape:\n"
        "{\n"
        '  "document_type": "string",\n'
        '  "pet_name": "string|null",\n'
        '  "visit_date": "YYYY-MM-DD|null",\n'
        '  "clinic_name": "string|null",\n'
        '  "diagnoses": ["string"],\n'
        '  "medications": ["string"],\n'
        '  "measurements": [{"name":"string","value":"string","unit":"string|null"}],\n'
        '  "notes": "string|null"\n'
        "}\n"
        "If a field is unavailable, use null or empty array.\n\n"
        f"TEXT:\n{raw_text}"
    )

    response = model.generate_content(
        prompt,
        generation_config=GenerationConfig(
            temperature=0.1,
            max_output_tokens=8000,
            response_mime_type="application/json",
        ),
    )
    return _extract_json_object(response.text or "{}")

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
    job_key = f"{bucket}/{blob_path}#{generation or ''}"
    task_hash = hashlib.sha256(job_key.encode("utf-8")).hexdigest()[:32]
    task_id = f"process-{task_hash}"
    task_name = client.task_path(project, location, queue, task_id)
    task["name"] = task_name

    try:
        created = client.create_task(request={"parent": parent, "task": task})
        return created.name
    except gcp_exceptions.AlreadyExists:
        logger.info(
            "Cloud Task already exists (dedup); treating as success task=%s job_key=%s pubsub_message_id=%s",
            task_name,
            job_key,
            pubsub_message_id,
        )
        return task_name


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

    MEDICAL_RECORD_PREFIX = "medical_records/"

    if not blob_path.startswith(MEDICAL_RECORD_PREFIX):
        logger.info("Ignoring non-medical-record path: gs://%s/%s", bucket, blob_path)
        return {}
    
    if not (_is_pdf_path(blob_path) or _is_supported_image_path(blob_path)):
        logger.info("Ignoring unsupported file type at pubsub layer: gs://%s/%s", bucket, blob_path)
        return {}
        
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

    is_pdf = _is_pdf_path(blob_path)
    is_image = _is_supported_image_path(blob_path)
    if not is_pdf and not is_image:
        logger.info("Skipping unsupported object type: gs://%s/%s", bucket, blob_path)
        return {"status": "skipped", "reason": "unsupported_file_type"}

    logger.info("Processing file: gs://%s/%s gen=%s", bucket, blob_path, generation)

    try:
        if is_pdf:
            # Attempt parser-based extraction first; OCR only when text is insufficient.
            parsed_text = _extract_text_with_pdf_reader(bucket=bucket, blob_path=blob_path, generation=generation)
            extraction_mode = "pdf_parser"

            if _needs_ocr(parsed_text):
                logger.info("PDF parser yielded insufficient text; running OCR for gs://%s/%s", bucket, blob_path)
                task_id = hashlib.sha256(f"{bucket}/{blob_path}#{generation or ''}".encode("utf-8")).hexdigest()[:32]
                parsed_text = _extract_text_with_vision_ocr(bucket=bucket, blob_path=blob_path, task_id=task_id)
                extraction_mode = "ocr_pdf"
        else:
            parsed_text = _extract_text_from_image_with_vision(
                bucket=bucket,
                blob_path=blob_path,
                generation=generation,
            )
            extraction_mode = "ocr_image"
        
        # Fail explicitly when both parser and OCR produce no usable text
        if not parsed_text:
            raise HTTPException(status_code=422, detail="Failed to extract text from file")

        logger.info(
            "Text extraction complete mode=%s text=%s chars=%s messageId=%s",
            extraction_mode,
            parsed_text,
            len(parsed_text),
            pubsub_message_id,
        )

        structured_data = _extract_structured_data_with_vertex(parsed_text)
        #logger.info("Structured JSON data extracted with vertex ai=%s", structured_data)
        logger.info("Vertex extraction complete messageId=%s", pubsub_message_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("File processing failed for gs://%s/%s: %s", bucket, blob_path, e)
        raise HTTPException(status_code=500, detail="File processing failed")

    db_info: Optional[dict] = None
    try:
        db_info = _persist_structured_data_to_db(blob_path=blob_path, structured=structured_data)
        logger.info(
            "DB persist complete record_id=%s labs=%s messageId=%s",
            (db_info or {}).get("medical_record_id"),
            (db_info or {}).get("lab_results_inserted"),
            pubsub_message_id,
        )
    except Exception as e:
        logger.exception("DB persist failed for gs://%s/%s: %s", bucket, blob_path, e)
        # Best-effort failure marking if we can identify the record without relying on an open txn.
        try:
            if db_pool is not None:
                with db_pool.connect() as conn:
                    with conn.begin():
                        record_id = _find_medical_record_id_by_blob_path(conn=conn, blob_path=blob_path)
                        if record_id:
                            _mark_medical_record_failed(conn=conn, record_id=record_id, message=str(e))
        except Exception:
            logger.exception("Failed to mark medical_records FAILED for gs://%s/%s", bucket, blob_path)

        raise HTTPException(status_code=500, detail="Database update failed")

    return {
        "status": "processed",
        "extraction_mode": extraction_mode,
        "text_length": len(parsed_text),
        "structured_data": structured_data,
        "db": db_info,
    }
