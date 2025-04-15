import os
import io
import csv
import json
from google.cloud import documentai_v1 as documentai
from google.oauth2 import service_account


def load_config(config_file="config.json"):
    """Loads configuration from a JSON file."""
    try:
        with open(config_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return None  # Or provide default config if appropriate
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{config_file}'.")
        return None


def process_document_ai(
    project_id: str,
    location: str,
    processor_id: str,
    file_path: str,
    mime_type: str = "application/pdf",
    credentials_path: str = None
) -> documentai.Document:
    """Processes a document using Google Cloud Document AI."""

    # Instantiates a client
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client_options = {"api_endpoint": f"{location}-documentai.googleapis.com"}
        client = documentai.DocumentProcessorServiceClient(client_options=client_options, credentials=credentials)
    else:  # Rely on GOOGLE_APPLICATION_CREDENTIALS
        client_options = {"api_endpoint": f"{location}-documentai.googleapis.com"}
        client = documentai.DocumentProcessorServiceClient(client_options=client_options)

    # The full resource name of the processor, e.g.:
    # projects/{project_id}/locations/{location}/processors/{processor_id}
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    # Read the file into memory
    with open(file_path, "rb") as f:
        image_content = f.read()

    # Configure the process request
    document = {"content": image_content, "mime_type": mime_type}

    # Set the request for processing the document
    request = documentai.ProcessRequest(name=name, document=document)

    # Recognizes text in the document
    result = client.process_document(request=request)
    return result.document


def extract_form_data(document: documentai.Document):
    """Extracts form data (field names and values) from a Document AI Document object."""

    page_data = []
    for page in document.pages:
        page_info = {"page_number": page.page_number + 1, "fields": []}  # Page numbers are 0-indexed
        for field in page.form_fields:
            field_name = field.field_name.text_anchor.content.strip()
            field_value = field.field_value.text_anchor.content.strip()
            page_info["fields"].append({"field_name": field_name, "field_value": field_value})
        page_data.append(page_info)
    return page_data


def convert_to_csv(data: list[dict]):
    """Converts the extracted form data to CSV format."""

    output = io.StringIO()
    csv_writer = csv.writer(output)

    # Write the header row
    header = ["Page Number", "Field Name", "Field Value"]
    csv_writer.writerow(header)

    for page_data in data:
        page_number = page_data["page_number"]
        for field in page_data["fields"]:
            csv_writer.writerow([page_number, field["field_name"], field["field_value"]])

    return output.getvalue()


def main():
    """Main function to process the document, extract form data, convert to CSV, and save to a file."""

    config = load_config()
    if not config:
        return  # Exit if config loading failed

    try:
        document = process_document_ai(
            config["project_id"],
            config["location"],
            config["processor_id"],
            config["file_path"],
            credentials_path=config.get("credentials_path")  # Optional
        )
        extracted_data = extract_form_data(document)
        csv_data = convert_to_csv(extracted_data)

        with open(config["output_csv_path"], "w", newline="") as f:
            f.write(csv_data)

        print(f"Successfully processed document and saved output to {config['output_csv_path']}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()