import requests
import re
import http.client
from urllib.parse import urlparse, urljoin
import logging
import os

# Set up logging
logger = logging.getLogger(__name__)


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
            new_url = urljoin(url, location)
            return handle_protected_url(new_url)
        elif response.status == 200:
            return url
        else:
            logger.error(f"Error following redirect: HTTP status {response.status}")
            raise Exception(f"Error following redirect: HTTP status {response.status}")
    except Exception as e:
        logger.error(f"Error processing URL {url}: {e}")
        raise Exception(f"Error processing URL {url}: {e}")
    finally:
        connection.close()


def download_file_from_google_drive(url, destination_folder):
    final_url = handle_protected_url(url)

    file_id = get_file_id_from_url(final_url)
    if not file_id:
        logger.error("Invalid Google Drive URL")
        return

    # Ensure the destination directory exists
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    URL = "https://drive.google.com/uc?export=download"

    with requests.Session() as session:
        response = session.get(URL, params={"id": file_id}, stream=True)
        token = get_confirm_token(response)

        if token:
            params = {"id": file_id, "confirm": token}
            response = session.get(URL, params=params, stream=True)

        # Attempt to get the filename from the response headers
        file_name = get_filename_from_cd(response.headers.get("content-disposition"))
        if not file_name:
            logger.error("Could not determine file name")
            return

        destination_path = os.path.join(destination_folder, file_name)
        save_response_content(response, destination_path)


def get_file_id_from_url(url):
    match = re.search(r"/file/d/([a-zA-Z0-9_-]+)/view", url)
    return match.group(1) if match else None


def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value
    return None


def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    logger.info("Trying to save to:", destination)  # Add this line for debugging
    try:
        with open(destination, "wb") as f:
            for chunk in response.iter_content(CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    except PermissionError as e:
        logger.error(f"Permission Error: {e}")


def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename="(.+)"', cd)
    if len(fname) == 0:
        return None
    return fname[0]
