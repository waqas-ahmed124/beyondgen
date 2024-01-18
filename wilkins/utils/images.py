from PIL import Image
import numpy as np
import cv2
import glob
import os


def extract_images_and_filenames(directory):
    """Extracts image file names and image data from all image files in the directory."""
    image_data = {
        "content": [],
        "images": [],
    }

    # Supported image extensions
    image_extensions = (".png", ".jpg", ".jpeg")

    # Search for image files in the directory
    for file_path in glob.glob(os.path.join(directory, "*")):
        if file_path.lower().endswith(image_extensions):
            # Get filename from the file path
            file_name = os.path.basename(file_path)
            image_data["content"].append(file_name)

            # Open the file in binary mode, read its content, and convert to PIL Image
            with open(file_path, "rb") as file:
                image_bytes = file.read()
                nparr = np.frombuffer(image_bytes, np.uint8)
                image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                pil_image = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
                image_data["images"].append(pil_image)

    return image_data
