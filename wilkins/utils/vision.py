import logging
import cv2
import numpy as np
from PIL import Image
import hashlib

logger = logging.getLogger(__name__)


def is_billboard(image):
    # Convert the image to grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Adaptive thresholding for more robust edge detection
    thresh = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY_INV, 11, 2
    )
    edges = cv2.Canny(thresh, 50, 150)

    # Find contours
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    for contour in contours:
        # Filter based on contour area to ignore very small/large contours
        area = cv2.contourArea(contour)
        if (
            area < 500 or area > gray.shape[0] * gray.shape[1] * 0.5
        ):  # Adjust thresholds as needed
            continue

        # Polygon approximation
        epsilon = 0.02 * cv2.arcLength(contour, True)
        approx = cv2.approxPolyDP(contour, epsilon, True)

        if len(approx) == 4:
            x, y, w, h = cv2.boundingRect(approx)
            aspect_ratio = max(w, h) / float(min(w, h))

            if 0.3 < aspect_ratio < 5:
                # Enhanced texture analysis
                if has_significant_texture(gray[y : y + h, x : x + w], area):
                    return True
    return False


def has_significant_texture(region, area):
    # Apply a Laplacian filter for edge enhancement
    laplacian_var = cv2.Laplacian(region, cv2.CV_64F).var()

    # Adjust the texture threshold based on the area of the contour
    # Larger billboards might have relatively less dense texture
    texture_threshold = (
        100 if area < 10000 else 50
    )  # Example thresholds, adjust as needed

    return laplacian_var > texture_threshold


def is_urban_scene(image):
    # Convert the image to grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Edge detection
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)

    # Hough Line Transform
    lines = cv2.HoughLines(edges, 1, np.pi / 180, 200)

    vertical_lines = 0
    horizontal_lines = 0

    if lines is not None:
        for line in lines:
            for rho, theta in line:
                a = np.cos(theta)
                b = np.sin(theta)
                if b == 0:  # vertical line
                    vertical_lines += 1
                elif a == 0:  # horizontal line
                    horizontal_lines += 1

    # Sky Detection
    upper_part = edges[0 : int(gray.shape[0] * 0.3), :]
    variance = np.var(upper_part)

    # Heuristic thresholds
    is_sky = variance < 300
    has_structures = vertical_lines > 5 or horizontal_lines > 5

    return is_sky and has_structures


def is_map(image):
    # Color variance check (maps might have limited color palette)
    variance = np.var(image)
    if variance < 1200:
        return True

    return False


def score_image(image):
    """Scores the image based on different criteria."""
    score = 0
    if is_urban_scene(image):
        score += 1  # Weight for urban scene
    if is_billboard(image):
        score += 1  # Weight for billboard
    if not is_map(image):
        score += 1  # Weight for map
    if not is_google_map(image):
        score += 1
    return score


def score_image_bus(image):
    """Scores the image based on different criteria."""
    score = 0
    if is_bus(image):
        score += 1  # Weight for billboard
    if not is_google_map(image):
        score += 1
    return score


def filter_media(pdf_images):
    logger.info("Filtering Images")
    dups_removed = remove_all_duplicates(pdf_images)
    media_images = []
    image_dimension_threshold = 300

    for image_list in dups_removed:
        highest_score = -1
        best_image = None

        for image_index, image_bytes in enumerate(image_list):
            if image_bytes is not None:
                nparr = np.frombuffer(image_bytes, np.uint8)
                image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

                # Get image dimensions
                h, w = image.shape[:2]

                # Check against threshold
                if w < image_dimension_threshold or h < image_dimension_threshold:
                    logger.info(f"Skipping image is too small {image_index}")
                    continue

                # Calculate the score for the image
                score = score_image(image)
                if score > highest_score:
                    highest_score = score
                    best_image = image

        if best_image is not None:
            image_rgb = cv2.cvtColor(best_image, cv2.COLOR_BGR2RGB)
            pil_image = Image.fromarray(image_rgb)
            media_images.append(pil_image)
        else:
            media_images.append(None)

    return media_images


def is_google_map(image):
    # Convert to grayscale for pattern recognition
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Check for grid-like patterns (roads, etc.)
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)
    lines = cv2.HoughLinesP(
        edges, 1, np.pi / 180, threshold=100, minLineLength=100, maxLineGap=10
    )
    grid_like_structures = len(lines) if lines is not None else 0

    # Color analysis for typical map colors
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)

    # Define color ranges for typical map colors
    # Note: These ranges might need adjustment
    lower_blue = np.array([110, 50, 50])
    upper_blue = np.array([130, 255, 255])
    blue_mask = cv2.inRange(hsv, lower_blue, upper_blue)
    blue_area = cv2.countNonZero(blue_mask)

    lower_green = np.array([50, 100, 100])
    upper_green = np.array([70, 255, 255])
    green_mask = cv2.inRange(hsv, lower_green, upper_green)
    green_area = cv2.countNonZero(green_mask)

    # Simple heuristic-based decision rule
    if grid_like_structures > 5 and blue_area > 500 and green_area > 500:
        return True
    else:
        return False


def remove_all_duplicates(page_images):
    """
    Removes all duplicates from a list of page images. All duplicates, including the first occurrence, are replaced with None.

    :param page_images: List of lists containing images for each page.
    :return: List of lists with all duplicates replaced by None.
    """
    image_hash_counts = {}
    unique_images = []

    # First pass: Count occurrences of each image
    for images in page_images:
        for image_bytes in images:
            if image_bytes is not None:
                # Check if image_bytes is already a bytes object
                if not isinstance(image_bytes, bytes):
                    # If it's a list of integers, convert it to bytes
                    if isinstance(image_bytes, list):
                        image_bytes = bytes(image_bytes)
                    else:
                        continue
                img_hash = hashlib.md5(image_bytes).hexdigest()
                image_hash_counts[img_hash] = image_hash_counts.get(img_hash, 0) + 1

    # Second pass: Replace duplicates with None
    for images in page_images:
        page_images_with_none = []
        for image_bytes in images:
            if image_bytes is not None:
                # Check if image_bytes is already a bytes object
                if not isinstance(image_bytes, bytes):
                    # If it's a list of integers, convert it to bytes
                    if isinstance(image_bytes, list):
                        image_bytes = bytes(image_bytes)
                img_hash = hashlib.md5(image_bytes).hexdigest()
                # Replace duplicate image with None
                if image_hash_counts[img_hash] > 1:
                    page_images_with_none.append(None)
                else:
                    page_images_with_none.append(image_bytes)
            else:
                page_images_with_none.append(None)  # Maintain None if originally None

        unique_images.append(page_images_with_none)

    return unique_images


def filter_media_bus(pdf_images):
    logger.info("Filtering Images")
    dups_removed = remove_all_duplicates(pdf_images)
    media_images = []
    image_dimension_threshold = 300

    for image_list in dups_removed:
        highest_score = 1
        best_image = None

        for image_index, image_bytes in enumerate(image_list):
            if image_bytes is not None:
                nparr = np.frombuffer(image_bytes, np.uint8)
                image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

                # Get image dimensions
                h, w = image.shape[:2]

                # Check against threshold
                if w < image_dimension_threshold or h < image_dimension_threshold:
                    logger.info(f"Skipping image is too small {image_index}")
                    continue

                # Calculate the score for the image
                score = score_image_bus(image)
                if score > highest_score:
                    highest_score = score
                    best_image = image

        if best_image is not None:
            image_rgb = cv2.cvtColor(best_image, cv2.COLOR_BGR2RGB)
            pil_image = Image.fromarray(image_rgb)
            media_images.append(pil_image)

    return media_images


def is_bus(image):
    # Convert the image to grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Apply Gaussian blur
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)

    # Edge detection
    edged = cv2.Canny(blurred, 50, 150)

    # Find contours
    contours, _ = cv2.findContours(
        edged.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )

    for contour in contours:
        # Filter based on contour area to ignore very small/large contours
        area = cv2.contourArea(contour)
        if area < 1000 or area > 50000:  # Adjust thresholds as needed
            continue

        # Approximate the contour
        epsilon = 0.01 * cv2.arcLength(contour, True)
        approx = cv2.approxPolyDP(contour, epsilon, True)

        # Bounding rectangle
        x, y, w, h = cv2.boundingRect(approx)
        aspect_ratio = w / float(h)

        # Assumptions for a bus shape
        if len(approx) >= 4 and 1.5 < aspect_ratio < 4.0:
            # Additional checks can be added here (like color filtering)
            return True

    return False
