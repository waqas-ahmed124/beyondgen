import extract_msg
import re
import os
import requests
import logging
from bs4 import BeautifulSoup
import glob
import urllib.parse
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

logger = logging.getLogger(__name__)


def parse_msg_file(msg_file_path, attachment_dir):
    url_pattern = r'(https?://[^\s<>"]+|www\.[^\s<>"]+)'
    # Open the .msg file
    with extract_msg.Message(msg_file_path) as msg:
        # Extract the email body text
        body = msg.body if msg.body else msg.htmlBody

        # Find all URLs in the body text
        links = re.findall(url_pattern, body)

        # List to hold attachment filenames
        attachments_list = []

        # Extract attachments
        for attachment in msg.attachments:
            file_path = os.path.join(attachment_dir, attachment.longFilename)
            # Write the attachment content to a file
            with open(file_path, "wb") as f:
                f.write(attachment.data)
            attachments_list.append(file_path)

        return attachments_list, links


def directory_contains_pdfs(attachment_dir):
    # Check if the directory exists
    if not os.path.exists(attachment_dir):
        logging.info(f"The directory {attachment_dir} does not exist.")
        return False

    # List all files in the directory
    for file in os.listdir(attachment_dir):
        if file.lower().endswith(".pdf"):
            return True  # Found a PDF, return True

    # No PDFs found in the directory
    return False


def check_link_content_type(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        content_type = response.headers.get("Content-Type", "").lower()
        if "application/pdf" in content_type:
            return "PDF document"
        elif "text/html" in content_type:
            return "HTML page"
        else:
            return "Unknown content type"
    except requests.RequestException as e:
        return f"Error checking the link: {e}"


def fetch_files_and_extract_links(directory):
    """
    Fetch all .msg and .html files from a directory and extract links from them.

    :param directory: The directory to search in
    :return: Dictionary of file paths and their extracted links
    """
    file_paths = glob.glob(
        os.path.join(directory, "**", "*.msg"), recursive=True
    ) + glob.glob(os.path.join(directory, "**", "*.html"), recursive=True)

    all_links = {}
    for file_path in file_paths:
        links = extract_links(file_path)
        all_links[file_path] = links

    return all_links


def extract_and_decode_urls(text):
    url_pattern = r'https?://[^\s<>"\']+|www\.[^\s<>"\']+'
    found_links = re.findall(url_pattern, text)

    decoded_links = set()  # Using a set to automatically remove duplicates
    for link in found_links:
        # if 'linkprotect.cudasvc.com' in link:
        #     # Skip protected links
        #     continue

        parsed_link = urllib.parse.urlparse(link)
        query_params = urllib.parse.parse_qs(parsed_link.query)
        target_url = query_params.get("url")

        if target_url:
            decoded_url = urllib.parse.unquote(target_url[0])
            decoded_links.add(decoded_url)
        else:
            decoded_links.add(link)

    return list(decoded_links)  # Convert back to list for consistency


def extract_links(file_path):
    """
    Extract links from either a .msg or .html file.

    :param file_path: Path to the .msg or .html file
    :return: List of extracted links
    """
    body = ""

    # Determine the file type (.msg or .html)
    if file_path.lower().endswith(".msg"):
        # Process .msg file
        with extract_msg.Message(file_path) as msg:
            body = msg.body
    elif file_path.lower().endswith(".html"):
        # Process .html file
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                soup = BeautifulSoup(file, "lxml")
                body = soup.get_text()
        except UnicodeDecodeError:
            try:
                with open(file_path, "r", encoding="latin1") as file:
                    soup = BeautifulSoup(file, "lxml")
                    body = soup.get_text()
            except UnicodeDecodeError as e:
                # Handle the case where the file can't be decoded
                logger.error(f"Error decoding {file_path}: {e}")

    links = extract_and_decode_urls(body)
    return links


def classify_all_links(links_dict):
    classified_links_dict = {}

    for file_path, links in links_dict.items():
        classified_links = {
            "pdf_links": [],
            "epitch_links": [],
            "dropbox_links": [],
            "barrett_links": [],
            "google_drive_links": [],
        }

        epitch_visited_pages = set()

        for link in links:
            decoded_link = decode_safe_link(link).lower()

            # Normalize the URL
            normalized_link = normalize_url(decoded_link)

            # Check if the link is from Dropbox
            if "dropbox.com" in normalized_link:
                classified_links["dropbox_links"].append(link)

            # Check if the link ends with '.pdf'
            elif normalized_link.endswith(".pdf"):
                classified_links["pdf_links"].append(link)

            # Check for epitch links
            elif any(
                keyword in normalized_link
                for keyword in ["epitch", "pitch", "presentation"]
            ):
                core_url = get_core_url(normalized_link)
                if core_url not in epitch_visited_pages:
                    epitch_visited_pages.add(core_url)
                    classified_links["epitch_links"].append(link)

            # Check for Barrett links
            elif "barrettoutdoor.com" in normalized_link:
                classified_links["barrett_links"].append(link)

            # Check for Google Drive links
            elif "drive.google.com" in normalized_link:
                classified_links["google_drive_links"].append(link)

        classified_links_dict[file_path] = classified_links

    return classified_links_dict


def normalize_url(url):
    """
    Normalize a URL by decoding it and removing query parameters that do not affect the page content.
    """
    parsed_url = urlparse(url)
    # Only include parameters that affect the page content
    # Modify as needed based on your understanding of the URL structure
    filtered_query = parse_qs(parsed_url.query)
    relevant_params = {
        k: filtered_query[k] for k in filtered_query if k in ["id", "page", "article"]
    }
    normalized_query = urlunparse(
        parsed_url._replace(query=urlencode(relevant_params, doseq=True))
    )
    return normalized_query


def get_core_url(url):
    parsed_url = urlparse(url)
    core_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
    return core_url


def decode_safe_link(link):
    """
    Decodes a safe link to extract the actual URL.
    """
    parsed_link = urlparse(link)
    query_params = parse_qs(parsed_link.query)

    # Update this part to better handle the specific structure of your protected links
    # 'a' parameter seems to hold the actual URL in encoded form
    target_url = query_params.get("a")
    if target_url:
        decoded_url = urllib.parse.unquote(target_url[0])
        return decoded_url
    else:
        return link
