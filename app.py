from flask import Flask, request, jsonify
import os
from process_data import process_data
from pyspark.sql import SparkSession
import requests
import uuid
from urllib.parse import urlparse
from pathlib import Path

app = Flask(__name__)

def create_processed_folder():
    unique_id = str(uuid.uuid4())[:8]  # Extracting the first 8 characters of the UUID
    folder_path = f"processed_data/{unique_id}"
    os.makedirs(folder_path)
    return folder_path

def is_url(input_str):
    return input_str.startswith("http://") or input_str.startswith("https://")

def is_local_path(input_str):
    return os.path.exists(input_str)

def download_file(url, local_path):
    response = requests.get(url)
    response.raise_for_status()
    with open(local_path, 'wb') as file:
        file.write(response.content)

def process_csv(input_path, folder_path):
    try:
        if is_url(input_path):
            # Download the file from the URL
            filename = os.path.basename(urlparse(input_path).path)
            local_path = f"{folder_path}/{filename}"
            download_file(input_path, local_path)
        elif is_local_path(input_path):
            # Assume it's a local file path
            filename = os.path.basename(input_path)
            local_path = input_path
        else:
            raise ValueError("Invalid input. Please provide a valid URL or local file path.")

        spark = SparkSession.builder.getOrCreate()
        processed_df,invalid_name_count,empty_airline_code_count= process_data(spark, local_path)

        cleaned_csv_path = f"{folder_path}/cleaned_data_{filename}"
        processed_df.toPandas().to_csv(cleaned_csv_path, index=False)

        summary = {
            "total_rows_processed": processed_df.count(),
            "invalid_name":invalid_name_count,
            "empty_airline_code":empty_airline_code_count
        }

        summary_path = f"{folder_path}/summary_{os.path.splitext(filename)[0]}.json"
        with open(summary_path, "w") as summary_file:
            summary_file.write(str(summary))

        # If data is local, return the full path
        if is_local_path(input_path):
            cleaned_csv_path = os.path.abspath(cleaned_csv_path)
            summary_path = os.path.abspath(summary_path)

        return cleaned_csv_path, summary_path, None

    except requests.exceptions.RequestException as e:
        return None, None, f"Error accessing URL: {str(e)}"
    except ValueError as e:
        return None, None, str(e)

@app.route('/processcsv', methods=['POST'])
def process_csv_endpoint():
    try:
        data = request.json
        data_path = data.get("data_url")

        if not data_path:
            return jsonify({"status": "error", "message": "Missing 'data_path' in request JSON payload"}), 400

        folder_path = create_processed_folder()

        cleaned_csv_path, summary_path, error = process_csv(data_path, folder_path)

        if error:
            return jsonify({"status": "error", "message": error}), 500

        return jsonify({
            "status": "success",
            "processed_data_url": cleaned_csv_path,
            "summary_url": summary_path,
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
