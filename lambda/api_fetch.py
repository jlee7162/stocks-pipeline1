# lambda/api_fetch.py
import os
import json
import boto3
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
TABLE = os.environ.get("TABLE_NAME")

# Optional: get secret
def get_secret(secret_name):
    if not secret_name:
        return None
    sm = boto3.client("secretsmanager")
    try:
        r = sm.get_secret_value(SecretId=secret_name)
        return r.get("SecretString")
    except Exception:
        return None

def handler(event, context):
    """
    If invoked by scheduled EventBridge rule: write a demo item to DynamoDB.
    If invoked by API Gateway GET /movers: return latest items from table.
    We'll detect invocation by HTTP (event has 'requestContext') for API, otherwise run ingest.
    """
    table = dynamodb.Table(TABLE)

    # if invoked by API Gateway (GET /movers)
    if isinstance(event, dict) and event.get("requestContext"):
        # scan table (ok for small dev tables); sort by timestamp desc and return top 7
        resp = table.scan()
        items = resp.get("Items", [])
        items_sorted = sorted(items, key=lambda x: x.get("timestamp", ""), reverse=True)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(items_sorted[:7])
        }

    # else: scheduled run -> write demo item (replace with real fetch/compute)
    now = datetime.utcnow().isoformat()
    demo_item = {
        "symbol": "DEMO",
        "timestamp": now,
        "price": 123.45,
        "pctChange": 0.0
    }
    table.put_item(Item=demo_item)
    return {"statusCode": 200, "body": json.dumps({"saved": demo_item})}