import os

from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID")

CSV_FILE_PATH = "data/CSV.csv"
CSV_FILE_SEP = ";"

OUTPUT_DIR = "outputs/"


