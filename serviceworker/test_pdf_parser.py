import argparse
import asyncio
import json

from app import tasks_process


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run serviceworker extraction test against a GCS object."
    )
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--blob-path", required=True, help="Blob path inside bucket")
    parser.add_argument("--generation", required=False, help="Optional object generation")
    args = parser.parse_args()

    payload = {
        "bucket": args.bucket,
        "blob_path": args.blob_path,
        "generation": args.generation,
        "pubsub_message_id": "local-test",
    }

    result = asyncio.run(tasks_process(payload))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()