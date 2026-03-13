import os
import json
import boto3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
TABLE = os.environ.get("TABLE_NAME")

def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def build_response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
            "Cache-Control": "max-age=300",
        },
        "body": json.dumps(body, default=decimal_serializer),
    }

def handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return build_response(200,{})

    table = dynamodb.Table(TABLE)
    today = datetime.now(timezone.utc).date()
    dates = [(today - timedelta(days=i)).isoformat() for i in range(7)]

    items = []
    for date in dates:
        try:
            resp = table.get_item(Key={"date": date})
            item = resp.get("Item")
            if item:
                items.append({
                    "date": item["date"],
                    "ticker": item["ticker"],
                    "pct_change": float(item["pct_change"]),
                    "open_price": float(item["open_price"]),
                    "close_price": float(item["close_price"]),
                    "direction": item.get("direction", "gain" if float(item["pct_change"]) >= 0 else "loss"),  
                })
        except Exception as e:
            print(f"Error fetching data for {date}: {e}")

    return build_response(200, {"data": items, "count": len(items)})