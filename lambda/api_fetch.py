# lambda/api_fetch.py
import os
import json
import time
import boto3
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from decimal import Decimal
dynamodb = boto3.resource("dynamodb")
TABLE = os.environ.get("TABLE_NAME")
SECRET = os.environ.get("MASSIVE_API_KEY_SECRET_ARN")

WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]
BASE_URL = "https://api.massive.com/v1/open-close/{ticker}/{date}"

def get_api_key()->str:
    sm = boto3.client("secretsmanager")
    r = sm.get_secret_value(SecretId=SECRET)
    return json.loads(r["SecretString"])["MASSIVE_API_KEY"]

def get_last_trade_day() -> str:
    today = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=1)
    if yesterday.weekday() == 5: #if saturday, go back to friday
        yesterday -= timedelta(days=1)
    elif yesterday.weekday() == 6: #if sunday, go back to friday
        yesterday -= timedelta(days=2)
    return yesterday.strftime("%Y-%m-%d")

def fetch_stock_data(ticker: str, api_key: str, date: str, retries: int=3) -> dict | None:
    url = BASE_URL.format(ticker=ticker, date=date) + f"?adjusted=true&apiKey={api_key}"

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read().decode())

            open_price = body.get("open")
            close_price = body.get("close")

            if not open_price or not close_price:
                print(f"[WARN] Missing OHLC for {ticker}")
                return None

            return {
                "ticker": ticker,
                "open": open_price,
                "close": close_price,
                "pct_change": ((close_price - open_price) / open_price) * 100,
            }

        except urllib.error.HTTPError as e:
            if e.code == 429:  # rate limited
                wait = 2 ** attempt  # 2s, 4s, 8s
                print(f"[WARN] Rate limited on {ticker}, retrying in {wait}s (attempt {attempt}/{retries})")
                time.sleep(wait)
            elif e.code in (401, 403):  # auth failure
                print(f"[ERROR] Auth failure on {ticker} — check API key. HTTP {e.code}")
                raise  # stop immediately, no point retrying
            else:
                print(f"[ERROR] HTTP {e.code} on {ticker}")
                return None

        except urllib.error.HTTPError as e:
            print(f"[ERROR] HTTP {e.code} on {ticker}")
            return None
        except Exception as e:
            print(f"[ERROR] Unexpected error on {ticker}: {e}")
            return None

def handler(event, context):
    #yesterday = "2025-03-07"
    yesterday = get_last_trade_day()
    api_key = get_api_key()
    
    stock_data, failed = [], []

    for ticker in WATCHLIST:
        result = fetch_stock_data(ticker, api_key, yesterday)
        if result:
            stock_data.append(result)
            print(f"[INFO] {ticker}: {result['pct_change']:+.2f}%")
        else:
            failed.append(ticker)
        time.sleep(0.8)

    if not stock_data:
        print("[ERROR] All tickers failed")
        return {"statusCode": 500, "body": json.dumps({"error": "All tickers failed"})}

    winner = max(stock_data, key=lambda x: abs(x["pct_change"]))
    print(f"[INFO] Winner: {winner['ticker']} ({winner['pct_change']:+.2f}%)")

    dynamodb.Table(TABLE).put_item(Item={
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
            "failed_tickers": failed,
        })
    }