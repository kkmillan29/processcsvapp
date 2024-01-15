
# ProcessCSVApp

ProcessCSVApp is a Flask application that provides an endpoint to process CSV data using PySpark. It includes functions to create a unique folder for storing processed data, determine if a given input is a URL or a local file path, download a file from a URL and save it locally, process a CSV file using PySpark, and save the cleaned data and a summary. The main endpoint (`/processcsv`) receives CSV data processing requests.

## Table of Contents

- [Getting Started](#getting-started)
- [Usage](#usage)
- [Endpoint](#endpoint)
- [Folder Structure](#folder-structure)
- [Dependencies](#dependencies)
- [Contributing](#contributing)

## Getting Started

These instructions will help you set up and run the Flask application on your local machine.

### Prerequisites

- Python 3.x
- [PySpark](https://pypi.org/project/pyspark/)
- [Flask](https://pypi.org/project/Flask/)

### Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/your_username/processcsvapp.git
    cd processcsvapp
    ```

2. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```
    or
   
   ```bash
    pip install -r requirements.txt --upgrade
    ```
   

4. Run the Flask application:
	
	For running the code in local:
    ```bash
    python app.py local
    ```
    For running the code in server
     ```bash
    python app.py server --serverurl http://server_url
    ```
   

The local application should be running at `http://127.0.0.1:5000/`.

## Usage

1. Send a POST request to the `/processcsv` endpoint with a JSON payload containing the CSV data URL or local file path.

    ```json
    {
        "data_url": "https://example.com/data.csv"
    }
    ```

    or

    ```json
    {
        "data_url": "/path/to/local/data.csv"
    }
    ```

2. The application will process the CSV data, create a unique folder for storing processed data, and return URLs for the cleaned CSV file and a summary.

## Endpoint

- **POST /processcsv:** Endpoint for processing CSV data.

    - **Request:**
        - Method: `POST`
        - JSON Payload:
        
            ```json
            {
                "data_url": "/path/to/local/data.csv"
            }
            ```
            or 
            ```json
            {
                "data_url": "https://example.com/data.csv"
            }
            ```

    - **Response:**
        - JSON Response when running in local:
            ```json
            {
                "status": "success",
                "processed_data_url": "/home/user/processed_data/<unique_id>/cleaned_data_data.csv",
                "summary_url": "/home/user/processed_data/<unique_id>/summary_data.json"
            }
            ```
	            
             JSON Response when running in server:
            ```json
            {
                "status": "success",
                "processed_data_url": "http://serverl_url/processed_data/<unique_id>/cleaned_data_data.csv",
                "summary_url": "http://serverl_url/processed_data/<unique_id>/summary_data.json"
            }
            ```
            
        - Error Response:
            ```json
            {
                "status": "error",
                "message": "Error message"
            }
            ```
 
## Folder Structure

- **processed_data:** Folder containing processed data for each request.

## Dependencies

- [PySpark](https://pypi.org/project/pyspark/): Apache Spark Python API.
- [Flask](https://pypi.org/project/Flask/): Web framework for building the application.

## Contributing

Feel free to contribute to this project. Fork the repository, make your changes, and submit a pull request.
