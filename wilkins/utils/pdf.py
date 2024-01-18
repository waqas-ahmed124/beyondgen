import os
import glob
import requests
import fitz
import logging

logger = logging.getLogger(__name__)


def extract_pdf_content_and_images(attachment_dir):
    """Extracts text content and images from all PDF files in the attachment directory."""
    pdf_data = {
        "content": [],
        "images": [],
    }
    pdf_files = glob.glob(os.path.join(attachment_dir, "**", "*.pdf"), recursive=True)
    for pdf_file in pdf_files:
        pdf_document = fitz.open(pdf_file)
        # Iterating through the pages in the pdf
        for page_number in range(len(pdf_document)):
            page = pdf_document[page_number]

            # Extract Text
            text = page.get_text()
            pdf_data["content"].append(text)

            # Extract Images
            images = page.get_images(full=True)
            image_bytes_list = []
            for _, img in enumerate(images):
                xref = img[0]
                base_image = pdf_document.extract_image(xref)
                image_bytes = base_image["image"]
                image_bytes_list.append(image_bytes)

            pdf_data["images"].append(image_bytes_list)

        pdf_document.close()

    return pdf_data


def download_pdfs(url_list, download_folder):
    """
    Download PDFs from a list of URLs.

    :param url_list: List of URLs pointing to PDF files.
    :param download_folder: The folder where PDFs will be downloaded.
    :return: List of paths to the downloaded files.
    """
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    downloaded_files = []

    for i, url in enumerate(url_list, start=1):
        try:
            response = requests.get(url)
            response.raise_for_status()

            # Correct filename determination
            filename = url.split("/")[-1]
            if not filename.lower().endswith(".pdf"):
                filename += ".pdf"

            file_path = os.path.join(download_folder, filename)
            with open(file_path, "wb") as f:
                f.write(response.content)

            downloaded_files.append(file_path)
            logger.info(f"Downloaded {i}/{len(url_list)}: {file_path}")
        except requests.exceptions.RequestException as e:
            logger.info(f"Failed to download {url}: {str(e)}")

    return downloaded_files


def delete_pdfs(self, file_paths):
    """Delete PDFs from local machine."""
    for file_path in file_paths:
        try:
            if os.path.exists(file_path) and file_path.lower().endswith(".pdf"):
                os.remove(file_path)
                logger.info(f"Deleted: {file_path}")
        except Exception as e:
            logger.info(f"Failed to delete {file_path}: {str(e)}")


def download_pdf_and_upload_to_azure(
    url, azure_storage, container_name, blob_name=None
):
    """
    Download a PDF from a URL and upload it to Azure Blob Storage.

    :param url: URL of the PDF file to download.
    :param azure_storage: Instance of AzureStorage class.
    :param container_name: Name of the Azure Blob Storage container where the file will be uploaded.
    :param blob_name: Name of the blob in Azure Storage. If None, the filename from the URL will be used.
    """
    try:
        # Download the PDF file
        response = requests.get(url)
        # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        response.raise_for_status()

        # Extract filename if blob_name not provided
        if blob_name is None:
            blob_name = url.split("/")[-1]

        # Ensure the file is a PDF
        if not blob_name.lower().endswith(".pdf"):
            logger.error(f"The file from the URL {url} is not a PDF.")
            return

        # Prepare file path for temporary storage
        # Using /tmp as a temporary folder
        temp_file_path = os.path.join("/tmp", blob_name)

        # Save the PDF file temporarily
        with open(temp_file_path, "wb") as file:
            file.write(response.content)

        # Upload the file to Azure Blob Storage
        azure_storage.upload_file(container_name, temp_file_path, blob_name)
        logger.info(
            f"Uploaded {blob_name} to Azure Blob Storage in container {container_name}"
        )

        # Optionally, delete the temporary file
        os.remove(temp_file_path)
        logger.info("Temporary file deleted.")

    except requests.RequestException as e:
        logger.error(f"Request error occurred: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
