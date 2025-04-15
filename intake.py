import json
import csv
import mimetypes
import os
from typing import List, Sequence

# Import Google Cloud client libraries using explicit credentials
from google.api_core.client_options import ClientOptions
from google.cloud import documentai

# --- Configuration Loading ---
CONFIG_FILE = "config.json"

def load_config(config_path: str) -> dict:
    """Loads configuration from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        # Basic validation
        required_keys = ["project_id", "location", "processor_id", "file_path", "output_csv_path", "credentials_path"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key '{key}' in {config_path}")
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        exit(1)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_path}'.")
        exit(1)
    except ValueError as e:
        print(f"Error: Configuration validation failed: {e}")
        exit(1)

# --- Document AI Processing ---

def _get_text(text_anchor: documentai.Document.TextAnchor, text: str) -> str:
    """Extract text from the document based on TextAnchor."""
    if not text_anchor.text_segments:
        return ""

    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in text_anchor.text_segments:
        start_index = int(segment.start_index)
        end_index = int(segment.end_index)
        response += text[start_index:end_index]
    return response.strip().replace("\n", " ") # Clean up whitespace

def process_document(
    project_id: str,
    location: str,
    processor_id: str,
    file_path: str,
    credentials_path: str,
    mime_type: str = None,
) -> documentai.Document:
    """
    Processes a document using the Document AI API with specified credentials.
    """
    print(f"Processing document: {file_path}")
    print(f"Using Processor: projects/{project_id}/locations/{location}/processors/{processor_id}")
    print(f"Using Credentials: {credentials_path}")

    # Ensure credentials file exists
    if not os.path.exists(credentials_path):
        print(f"Error: Credentials file '{credentials_path}' not found.")
        exit(1)

    # You must set the api_endpoint if you use a location other than 'us'.
    opts = ClientOptions(
        api_endpoint=f"{location}-documentai.googleapis.com",
        credentials_file=credentials_path # Explicitly pass credentials file path
    )

    try:
        client = documentai.DocumentProcessorServiceClient(client_options=opts)

        # The full resource name of the processor
        name = client.processor_path(project_id, location, processor_id)

        # Read the file into memory
        try:
            with open(file_path, "rb") as document_file:
                document_content = document_file.read()
        except FileNotFoundError:
            print(f"Error: Input document file '{file_path}' not found.")
            exit(1)

        # Guess MIME type if not provided
        if mime_type is None:
            mime_type, _ = mimetypes.guess_type(file_path)
            if mime_type is None:
                 # Default or raise error if crucial
                 print(f"Warning: Could not guess MIME type for {file_path}. Assuming 'application/pdf'.")
                 mime_type = "application/pdf" # Common default

        print(f"Detected MIME Type: {mime_type}")

        # Configure the process request
        raw_document = documentai.RawDocument(content=document_content, mime_type=mime_type)
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)

        # Use the Process API to extract structured data
        result = client.process_document(request=request)
        print("Document processing complete.")
        return result.document

    except Exception as e:
        print(f"An error occurred during Document AI processing: {e}")
        # Consider more specific error handling for API errors (e.g., google.api_core.exceptions)
        exit(1)

# --- CSV Conversion ---

def extract_form_data_to_csv(document: documentai.Document, output_csv_path: str):
    """
    Extracts form field names and values page by page and saves to a CSV file.
    """
    print(f"Extracting form data and saving to: {output_csv_path}")
    header = ["Page Number", "Field Name", "Field Value"]
    csv_data = [header]
    full_text = document.text

    for page_num, page in enumerate(document.pages):
        print(f"--- Processing Page {page_num + 1} ---")
        if not page.form_fields:
            print(f"  No form fields detected on page {page_num + 1}.")
            continue

        for field in page.form_fields:
            field_name = _get_text(field.field_name.text_anchor, full_text)
            field_value = _get_text(field.field_value.text_anchor, full_text)
            confidence_name = field.field_name.confidence
            confidence_value = field.field_value.confidence

            print(f"  Detected Field: '{field_name}' (Conf: {confidence_name:.2f}) = '{field_value}' (Conf: {confidence_value:.2f})")
            csv_data.append([page_num + 1, field_name, field_value])

    # Write data to CSV
    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csv_data)
        print(f"Successfully saved form data to {output_csv_path}")
    except IOError as e:
        print(f"Error writing to CSV file '{output_csv_path}': {e}")
        exit(1)

# --- Main Execution ---

if __name__ == "__main__":
    # 1. Load Configuration
    config = load_config(CONFIG_FILE)

    # 2. Process Document with Document AI
    processed_document = process_document(
        project_id=config["project_id"],
        location=config["location"],
        processor_id=config["processor_id"],
        file_path=config["file_path"],
        credentials_path=config["credentials_path"]
        # mime_type can be optionally specified here if needed
    )

    # 3. Extract data and save to CSV
    if processed_document:
        extract_form_data_to_csv(processed_document, config["output_csv_path"])
    else:
        print("Document processing failed, cannot extract data.")
        exit(1)