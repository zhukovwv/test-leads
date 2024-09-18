import json
import os
from pathlib import Path

import pandas as pd
import requests
from prefect import task
from dotenv import load_dotenv


load_dotenv()

TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID")


def load_csv(file_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path, sep=",")
    except Exception as e:
        raise RuntimeError(f"Ошибка при загрузке CSV файла: {e}")


@task(retries=3, retry_delay_seconds=5)
def fetch_data_from_api(row: pd.Series) -> dict:
    api_url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": row['symbol'],
        "apikey": "demo"
    }

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Ошибка при выполнении запроса: {e}")


@task
def process_api_data(data: dict) -> tuple[pd.DataFrame, dict]:
    meta_data: dict = data.get("Meta Data", {})
    time_series: dict = data.get("Time Series (Daily)", {})

    df: pd.DataFrame = pd.DataFrame.from_dict(time_series, orient='index')
    df.index = pd.to_datetime(df.index)
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'date'}, inplace=True)

    df = df.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    })

    df = df.apply(pd.to_numeric, errors='coerce')
    return df, meta_data


@task
def save_data_to_json(data: pd.DataFrame, meta_data: dict, output_dir: str, file_name: str) -> None:
    output_path = Path(output_dir) / f"{file_name}.json"
    data_dict: dict = data.to_dict(orient="records")

    combined_data: dict = {
        "meta_data": meta_data,
        "time_series_daily": data_dict
    }

    try:
        with open(output_path, 'w') as file:
            json.dump(combined_data, file, indent=4, default=str)
    except Exception as e:
        raise RuntimeError(f"Ошибка при сохранении файла: {e}")


@task
def send_telegram_message(message: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    params = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    try:
        response = requests.post(url, params=params)
        response.raise_for_status()
        result = response.json()

        if not result.get("ok"):
            raise RuntimeError(f"Ошибка при отправке сообщения: {result.get('description')}")
    except requests.RequestException as e:
        raise RuntimeError(f"Ошибка при отправке сообщения в Telegram: {e}")
