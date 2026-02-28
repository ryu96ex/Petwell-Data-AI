import base64
import json
from flask import Blueprint, request, abort

pubsub_bp = Blueprint("pubsub", __name__)

@pubsub_bp.post("/pubsub/push")
def pubsub_push():
    envelope = request.get_json(silent=True)
    if not envelope or "message" not in envelope:
        abort(400, "Invalid Pub/Sub envelope")

    msg = envelope["message"]
    data_b64 = msg.get("data")
    if not data_b64:
        abort(400, "Missing message.data")

    try:
        payload_bytes = base64.b64decode(data_b64)
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as e:
        abort(400, f"Bad data: {e}")

    bucket = payload.get("bucket") or payload.get("bucketId")
    name = payload.get("name") or payload.get("objectId")
    generation = payload.get("generation")
    message_id = msg.get("messageId")

    if not bucket or not name:
        abort(400, f"Missing bucket/name in payload: {payload}")

    # Keep this handler fast; enqueue real work elsewhere.
    print(f"Received: gs://{bucket}/{name} gen={generation} messageId={message_id}", flush=True)

    return ("", 204)
