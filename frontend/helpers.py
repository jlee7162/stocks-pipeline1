import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

API_URL = "https://gcl44jgl01.execute-api.us-east-1.amazonaws.com/prod/movers"  

def fetch_stock_data() -> pd.DataFrame:
    """Fetch from API, fall back to data.csv if unavailable."""
    try:
        resp = requests.get(API_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if data:
            df = pd.DataFrame(data)
            df.to_csv("data.csv", index=False)  # cache locally
            return df
    except Exception as e:
        print(f"[WARN] API fetch failed: {e}, falling back to data.csv")
    return pd.read_csv("data.csv")

def color_direction(direction: str) -> str:
    return "green" if direction == "gain" else "red"

def plot_pct_change(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 4))
    colors = [color_direction(d) for d in df["direction"]]
    bars = ax.bar(df["date"], df["pct_change"], color=colors, edgecolor="white")
    ax.axhline(0, color="gray", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Date")
    ax.set_ylabel("% Change")
    ax.set_title("Daily Top Mover % Change")
    plt.xticks(rotation=45)
    for bar, ticker in zip(bars, df["ticker"]):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + (0.05 if bar.get_height() >= 0 else -0.15),
                ticker, ha="center", va="bottom", fontsize=9, fontweight="bold")
    gain_patch = mpatches.Patch(color="green", label="Gain")
    loss_patch = mpatches.Patch(color="red", label="Loss")
    ax.legend(handles=[gain_patch, loss_patch])
    plt.tight_layout()
    plt.show()