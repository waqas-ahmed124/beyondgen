import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from PIL import Image
import io
import logging

logger = logging.getLogger(__name__)


def load_images_from_urls(url_lists):
    images_bytes_groups = []
    for urls in url_lists:
        images_bytes = []
        if len(urls) == 0:
            images_bytes.append(None)
        for url in urls:
            try:
                with requests.get(url, timeout=10) as response:
                    content_type = response.headers.get("Content-Type", "")

                    # Check if the content type is of an image
                    if response.status_code == 200 and "image" in content_type:
                        image = Image.open(io.BytesIO(response.content))
                        img_byte_arr = io.BytesIO()
                        image_format = image.format or "PNG"
                        image.save(img_byte_arr, format=image_format)
                        img_byte_arr.seek(0)  # Go to the start of the stream
                        images_bytes.append(img_byte_arr.getvalue())
                    else:
                        logger.warning(
                            f"URL did not point to an image or response was not successful. URL: {url}, Content-Type: {content_type}"
                        )
                        images_bytes.append(None)

            except Exception as e:
                logger.error(f"Error loading image from {url}: {e}")
                images_bytes.append(None)
        images_bytes_groups.append(images_bytes)
    return images_bytes_groups


def fetch_page_content(url):
    # Check if the URL has a scheme, and if not, prepend 'https://'
    parsed_url = urlparse(url)
    if not parsed_url.scheme:
        # url = 'https://' + url
        return None
    # Fetch the initial page content
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"Failed to load page: {url}")
        raise Exception(f"Failed to load page: {url}")

    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # Look for a <frame> tag
    frame = soup.find("frame")
    if frame and "src" in frame.attrs:
        # If a frame is found, construct the full URL for the frame
        # (assuming it's a relative path)
        base_url = response.url.rsplit("/", 1)[0]
        frame_url = frame["src"]
        if not frame_url.startswith(("http://", "https://")):
            frame_url = base_url + "/" + frame_url

        # Fetch the content of the frame
        frame_response = requests.get(frame_url)
        if frame_response.status_code != 200:
            logger.error(f"Failed to load frame content: {frame_url}")
            raise Exception(f"Failed to load frame content: {frame_url}")
        return frame_response.content
    else:
        # If no frame, return the original page content
        return response.content


def get_specific_base_url(url):
    # Check if the URL is protected by a link protection service
    if "linkprotect.cudasvc.com" in url:
        try:
            # Follow the redirect to get the actual URL
            response = requests.get(url, timeout=10, allow_redirects=True)
            response.raise_for_status()
            url = response.url  # Update the URL to the redirected one
        except Exception as e:
            logger.error(f"Error resolving protected URL {url}: {e}")
            return None  # or handle this as you see fit

    parsed_url = urlparse(url)
    path_segments = parsed_url.path.split("/")
    # Include the first non-empty segment of the path, if it exists
    if len(path_segments) > 1 and path_segments[1]:
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/{path_segments[1]}/"
    else:
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
    return base_url


def table_search_criteria(tag):
    return (
        tag.name == "table"
        and tag.get("border") == "0"
        and tag.get("cellpadding") == "1"
        and tag.get("cellspacing") == "1"
        and tag.get("width") == "300"
    )


def extract_tables_content(html_content, base_url):
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all tables with the specified attributes
    tables = soup.find_all(table_search_criteria)

    all_tables_text = []
    all_tables_images = []

    for table in tables:
        table_text = []
        table_images = []

        for element in table.find_all(["td", "img"]):
            if element.name == "img":
                # Extract image URL
                img_url = element["src"]

                # Check if the base URL is already in the img_url, if not, prepend it
                if not img_url.startswith(base_url):
                    img_url = base_url + img_url

                table_images.append(img_url)
            else:
                # Extract text
                text = element.get_text(separator="\n", strip=True)
                text = "\n".join(
                    line for line in text.split("\n") if line.strip() != ""
                )  # Removing excessive newlines
                table_text.append(text)

        all_tables_text.append(table_text)
        all_tables_images.append(table_images)

    return all_tables_text, all_tables_images


def extract_epitch_content_and_images(url):
    base_url = get_specific_base_url(url)
    page_source = fetch_page_content(url)
    text_content, image_url_groups = extract_tables_content(page_source, base_url)

    # Load images from grouped URLs
    images_bytes_groups = load_images_from_urls(image_url_groups)

    epitch_data = {
        "content": text_content,
        "images": images_bytes_groups,
    }
    return epitch_data


def extract_barrett_html(html_content, base_url):
    soup = BeautifulSoup(html_content, "html.parser")

    # Extract all text
    all_text = soup.get_text(separator="\n", strip=True)
    all_text = "\n".join(
        line for line in all_text.split("\n") if line.strip() != ""
    )  # Removing excessive newlines

    # Find the specific image
    image = soup.find("img", {"id": "billboard-image"})

    # Extract and adjust the image URL
    image_urls = []  # Initialize as an empty list
    if image:
        image_url = image["src"]
        if not image_url.startswith(("http://", "https://")):
            image_url = base_url + image_url
        image_urls.append(image_url)  # Append the URL to the list
    return all_text, image_urls


def extract_barrett_content_and_images(url_list):
    all_text = []
    all_images = []
    base_url = "https://barrettoutdoor.com/"
    for url in url_list:
        page_source = fetch_page_content(url)
        if page_source is not None:
            text_content, image_url = extract_barrett_html(page_source, base_url)
            if image_url:
                all_text.append(text_content)
                all_images.append(image_url)

    images_bytes_groups = load_images_from_urls(all_images)
    flattened_images_bytes = [
        item for sublist in images_bytes_groups for item in sublist
    ]

    barret_data = {
        "content": all_text,
        "images": flattened_images_bytes,
    }
    return barret_data
