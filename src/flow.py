from datetime import datetime
import uuid

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner

from config import CSV_FILE_PATH, OUTPUT_DIR, CSV_FILE_SEP
from utils import fetch_data_from_api, process_api_data, save_data_to_json, send_telegram_message, load_csv


@flow(task_runner=ThreadPoolTaskRunner(max_workers=3))
def data_processing_flow():
    df = load_csv(CSV_FILE_PATH, CSV_FILE_SEP)

    for idx, row in df.iterrows():
        api_data = fetch_data_from_api(row)
        data, meta_data = process_api_data(api_data)
        save_data_to_json(data, meta_data, OUTPUT_DIR, f"{datetime.now()}-{uuid.uuid4()}")

    send_telegram_message("Flow completed!")


if __name__ == "__main__":
    data_processing_flow.serve(name="data-processing-deployment")
