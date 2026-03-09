"""
lambda/ingestion.py
Triggered daily by EventBridge.
Fetches previous day OHLC from Massive API for each watchlist stock,
finds the top mover by absolute % change, writes result to DynamoDB.
"""

import os
import json
import time
import boto3
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from functools import lru_cache

WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
TABLE_NAME = os.environ["TABLE_NAME"]
SECRET_ARN = os.environ["MASSIVE_API_KEY_SECRET_ARN"]
BASE_URL = "https://api.massive.com/v1/open-close/{ticker}/{date}"

dynamodb = boto3.resource("dynamodb")
secrets_client = boto3.client("secretsmanager")


@lru_cache(maxsize=1)
def get_api_key() -> str:
    """Fetch API key from Secrets Manager. Cached per cold start."""
    resp = secrets_client.get_secret_value(SecretId=SECRET_ARN)
    return json.loads(resp["SecretString"])["MASSIVE_API_KEY"]


def fetch_stock_data(ticker: str, api_key: str, date: str, retries: int = 3) -> dict | None:
    """
    Fetch previous day OHLC for one ticker from Massive API.
    Returns dict with ticker/open/close/pct_change, or None on failure.
    """
    url = BASE_URL.format(ticker=ticker, date=date) + f"?adjusted=true&apiKey={api_key}"
    headers = {"Accept": "application/json"}

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read().decode())

            open_price = body.get("open")
            close_price = body.get("close")

            if not open_price or not close_price:
                print(f"[WARN] Missing OHLC for {ticker}: {body}")
                return None

            return {
                "ticker": ticker,
                "open": open_price,
                "close": close_price,
                "pct_change": ((close_price - open_price) / open_price) * 100,
            }

        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** attempt
                print(f"[WARN] Rate limited on {ticker}, retrying in {wait}s (attempt {attempt}/{retries})")
                time.sleep(wait)
            elif e.code in (401, 403):
                print(f"[ERROR] Auth failure on {ticker} — check API key. HTTP {e.code}")
                raise
            else:
                print(f"[ERROR] HTTP {e.code} on {ticker}: {e.reason}")
                return None

        except urllib.error.URLError as e:
            print(f"[ERROR] Network error on {ticker} (attempt {attempt}/{retries}): {e.reason}")
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return None

        except Exception as e:
            print(f"[ERROR] Unexpected error on {ticker}: {e}")
            return None

    return None

def get_last_trade_day() -> str:
    today = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=1)
    if yesterday.weekday() == 5:  # Saturday or Sunday
        yesterday -= timedelta(days=1)  # Move to Friday
    elif yesterday.weekday() == 6:
        yesterday -= timedelta(days=2)  # Move to Friday
    return yesterday.strftime("%Y-%m-%d")

def handler(event, context):
    yesterday = get_last_trade_day()
    print(f"[INFO] Ingestion started. Fetching data for {yesterday}")

    api_key = get_api_key()
    stock_data, failed = [], []

    for ticker in WATCHLIST:
        result = fetch_stock_data(ticker, api_key, yesterday)
        if result:
            stock_data.append(result)
            print(f"[INFO] {ticker}: open={result['open']}, close={result['close']}, pct={result['pct_change']:+.2f}%")
        else:
            failed.append(ticker)
        time.sleep(0.4)  # polite delay between requests

    if not stock_data:
        print("[ERROR] All tickers failed — aborting without writing to DynamoDB")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "All tickers failed", "failed": failed})
        }

    winner = max(stock_data, key=lambda x: abs(x["pct_change"]))
    print(f"[INFO] Winner: {winner['ticker']} ({winner['pct_change']:+.2f}%)")

    dynamodb.Table(TABLE_NAME).put_item(Item={
        "date": yesterday,
        "ticker": winner["ticker"],
        "pct_change": Decimal(str(round(winner["pct_change"], 4))),
        "close_price": Decimal(str(round(winner["close"], 4))),
        "open_price": Decimal(str(round(winner["open"], 4))),
        "direction": "gain" if winner["pct_change"] >= 0 else "loss",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    })

    return {
        "statusCode": 200,
        "body": json.dumps({
            "date": yesterday,
            "winner": winner["ticker"],
            "pct_change": round(winner["pct_change"], 4),
            "direction": "gain" if winner["pct_change"] >= 0 else "loss",
            "stocks_checked": len(stock_data),
            "failed_tickers": failed,
        })
    }