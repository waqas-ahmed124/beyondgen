import os
import requests
from zipfile import ZipFile
from io import BytesIO
import logging
import http.client
from urllib.parse import urlparse, unquote, urljoin, parse_qs

logger = logging.getLogger(__name__)


def is_zip_file_signature(data):
    return (
        data.startswith(b"PK\x03\x04")
        or data.startswith(b"PK\x05\x06")
        or data.startswith(b"PK\x07\x08")
    )


def download_and_extract_zip_azure(url, extract_to, azure_storage, container_name):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    final_url = handle_protected_url(url)
    url = get_final_dropbox_url(final_url)
    response = requests.get(url)
    if response.status_code == 200:
        if is_zip_file_signature(response.content):
            with ZipFile(BytesIO(response.content)) as zip_file:
                zip_file.extractall(extract_to)
            logger.info(f"ZIP file extracted to {extract_to}")
        else:
            # Decode and remove URL parameters
            file_name = unquote(url.split("/")[-1].split("?")[0])
            file_path = os.path.join(extract_to, file_name)
            with open(file_path, "wb") as file:
                file.write(response.content)
            logger.info(f"Non-ZIP file saved to {extract_to}")

        # Upload to Azure and clean up
        for filename in os.listdir(extract_to):
            file_path = os.path.join(extract_to, filename)
            azure_storage.upload_file(container_name, file_path)
            logger.info(
                f"Uploaded {filename} to Azure Blob Storage in container {container_name}"
            )
            os.remove(file_path)

        os.rmdir(extract_to)
        logger.info("Cleaned up extracted files")
    else:
        logger.error(f"Failed to download. Status code: {response.status_code}")


def download_and_extract_zip(url, extract_to):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    final_url = handle_protected_url(url)
    url = get_final_dropbox_url(final_url)
    response = requests.get(url)
    if response.status_code == 200:
        if is_zip_file_signature(response.content):
            with ZipFile(BytesIO(response.content)) as zip_file:
                zip_file.extractall(extract_to)
            logger.info(f"ZIP file extracted to {extract_to}")
        else:
            # Decode and remove URL parameters
            file_name = unquote(url.split("/")[-1].split("?")[0])
            file_path = os.path.join(extract_to, file_name)
            with open(file_path, "wb") as file:
                file.write(response.content)
            logger.info(f"Non-ZIP file saved to {extract_to}")
    else:
        logger.error(f"Failed to download. Status code: {response.status_code}")


def handle_protected_url(url):
    """Follow redirects and return the final URL."""
    parsed_url = urlparse(url)
    connection = (
        http.client.HTTPSConnection(parsed_url.netloc)
        if parsed_url.scheme == "https"
        else http.client.HTTPConnection(parsed_url.netloc)
    )

    try:
        connection.request("GET", parsed_url.path + "?" + parsed_url.query)
        response = connection.getresponse()

        if response.status in [301, 302, 303, 307]:
            location = response.getheader("Location")
            # Handle relative URLs for redirection
            new_url = urljoin(url, location)
            # Recursively follow the redirect
            return handle_protected_url(new_url)
        elif response.status == 200:
            return url  # No further redirection, return the original URL
        else:
            logger.error(f"Error following redirect: HTTP status {response.status}")
            raise Exception(f"Error following redirect: HTTP status {response.status}")
    except Exception as e:
        logger.error(f"Error processing URL {url}: {e}")
        raise Exception(f"Error processing URL {url}: {e}")
    finally:
        connection.close()


def get_final_dropbox_url(url):
    """Check if the URL is a Dropbox link and modify it for direct download."""
    if "dropbox.com" in url:
        return url.replace("dl=0", "dl=1")
    return url
