import base64
import json
import logging
import os
import re
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from google.cloud import tasks_v2
from google.api_core import exceptions as gcp_exceptions

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# Expected: pets/{pet_id}/records/{record_id}.pdf
_RECORD_PATH_RE = re.compile(r"^pets\/[^\/]+\/records\/([0-9a-fA-F-]{36})\.pdf$")


def parse_record_id(blob_path: str) -> str:
    m = _RECORD_PATH_RE.match(blob_path)
    if not m:
        raise HTTPException(status_code=400, detail=f"Unexpected blob_path format: {blob_path}")
    return m.group(1)


def enqueue_task(*, record_id: str, bucket: str, blob_path: str, generation: Optional[str], pubsub_message_id: Optional[str]) -> str:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    location = os.environ["TASKS_LOCATION"]
    queue = os.environ["TASKS_QUEUE"]
    worker_base_url = os.environ["WORKER_BASE_URL"]
    invoker_sa = os.environ.get("TASKS_INVOKER_SA")  # optional but recommended

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)

    task_payload = {
        "record_id": record_id,
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
    task_id = f"process-{record_id}"
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

    record_id = parse_record_id(blob_path)

    try:
        task_name = enqueue_task(
            record_id=record_id,
            bucket=bucket,
            blob_path=blob_path,
            generation=generation,
            pubsub_message_id=message_id,
        )
        logger.info("Enqueued task: %s", task_name)
    except gcp_exceptions.AlreadyExists:
        logger.info("Task already exists for record_id=%s (duplicate Pub/Sub). Acking.", record_id)
    except Exception as e:
        logger.exception("Failed to enqueue Cloud Task: %s", e)
        raise HTTPException(status_code=500, detail="Failed to enqueue Cloud Task")

    # Pub/Sub considers any 2xx an ack
    return {}


@app.post("/tasks/process")
async def tasks_process(payload: dict):
    record_id = payload.get("record_id")
    bucket = payload.get("bucket")
    blob_path = payload.get("blob_path")
    if not record_id or not bucket or not blob_path:
        raise HTTPException(status_code=400, detail=f"Missing record_id/bucket/blob_path: {payload}")

    # TODO: implement OCR + Vertex + DB updates
    logger.info("Processing record_id=%s gs://%s/%s", record_id, bucket, blob_path)
    return {}
