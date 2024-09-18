from prefect import flow
from utils import load_csv, fetch_data_from_api, process_api_data, save_data_to_json, send_telegram_message


@flow
def data_processing_flow(csv_file_path: str, output_dir: str):
    df = load_csv(csv_file_path)

    for idx, row in df.iterrows():
        api_data = fetch_data_from_api(row)
        data, meta_data = process_api_data(api_data)
        save_data_to_json(data, meta_data, output_dir, f"result_{idx}")

    send_telegram_message("Flow completed!")


if __name__ == "__main__":
    data_processing_flow("data/CSV.csv", "outputs/")
