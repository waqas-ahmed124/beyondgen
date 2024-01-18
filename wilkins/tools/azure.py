import os
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    BlobSasPermissions,
    BlobClient,
    ContainerClient,
)
import logging
import uuid
from PIL import Image
from datetime import datetime, timedelta
import io
from boxsdk import Client, JWTAuth
from boxsdk.exception import BoxAPIException
import json
import re
logger = logging.getLogger(__name__)


class AzureStorage:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

    def upload_file(self, container_name, file_path, blob_name=None):
        if blob_name is None:
            blob_name = os.path.basename(file_path)

        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Uploaded {file_path} to {container_name}/{blob_name}")

    def download_file(self, container_name, blob_name, download_folder):
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        download_file_path = os.path.join(download_folder, blob_name.replace("/", "_"))

        os.makedirs(download_folder, exist_ok=True)

        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
            logger.info(
                f"Downloaded {container_name}/{blob_name} to {download_file_path}"
            )

    def _download_all_blobs(self, container_name, download_folder):
        container_client = self.blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs()
        for blob in blobs:
            self.download_file(container_name, blob.name, download_folder)

    def list_blobs_in_container(self, container_name):
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()
        return [blob.name for blob in blob_list]

    def all_blobs_in_container(self, container_name):
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()
        return blob_list

    def create_container(self, container_name):
        try:
            # Get a client to interact with the specified container
            container_client = self.blob_service_client.get_container_client(
                container_name
            )

            # Check if the container already exists
            if not container_client.exists():
                # Create new container
                container_client.create_container()
                logger.info(f"Container '{container_name}' created.")
            else:
                logger.info(f"Container '{container_name}' already exists.")

            return container_client
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return None

    def getBlobClient(self, container_name, blob):
        return self.blob_service_client.get_blob_client(
            container=container_name, blob=blob.name
        )

    def process_blobs_recursive(self, container_name, prefix="", process_function=None):
        """
        Recursively process all blobs starting with a given prefix.

        :param container_name: Name of the Azure storage container.
        :param prefix: Prefix to filter blobs to process.
        :param process_function: A function to call with each blob name. If None, it will logging.info the blob names.
        """
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=prefix, delimiter="/")
        for blob in blob_list:
            # Process only if it's not a "folder"
            if not blob.name.endswith("/"):
                if process_function:
                    process_function(container_name, blob.name)
                else:
                    logging.info(f"Blob found: {blob.name}")

    def fetch_files_from_folder(
        self, container_name, folder_prefix, download_folder=None
    ):
        """
        Fetch all files within a specific folder in a container.

        :param container_name: Name of the Azure storage container.
        :param folder_prefix: The folder path within the container from where files should be fetched.
        :param download_folder: The local folder where the files should be downloaded (optional).
        """
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_iter = container_client.list_blobs(name_starts_with=folder_prefix)

        # Iterate through the blobs in the folder
        for blob in blob_iter:
            # This ensures we skip "folders" and only process "files"
            if not blob.name.endswith("/"):
                logger.info(f"Found blob: {blob.name}")
                if download_folder:
                    # If a download folder is provided, download the file
                    self.download_file(container_name, blob.name, download_folder)

    def upload_file_to_folder(
        self, container_name, folder_path, file_path, blob_name=None
    ):
        """
        Upload a file to a specific folder within a container.

        :param container_name: Name of the Azure storage container.
        :param folder_path: The folder path within the container where the file will be uploaded.
        :param file_path: The local path to the file.
        :param blob_name: The blob name. If None, the file name will be used.
        """
        if not folder_path.endswith("/"):
            folder_path += "/"

        if blob_name is None:
            blob_name = os.path.basename(file_path)

        # Prefix the blob_name with the folder path
        full_blob_name = f"{folder_path}{blob_name}"
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=full_blob_name
        )

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Uploaded {file_path} to {container_name}/{full_blob_name}")

    def generate_powerpoint_sas_urls(self, container_name, project, expiry_hours=24):
        """
        Generate SAS URLs for all PowerPoint files in the specified container.

        :param container_name: Name of the Azure storage container.
        :param expiry_hours: The number of hours for which the SAS URL will be valid.
        :return: A dictionary of PowerPoint file names and their SAS URLs.
        """
        powerpoint_sas_urls = {}
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=project)

        for blob in blob_list:
            if blob.name.lower().endswith((".ppt", ".pptx")):
                sas_blob = generate_blob_sas(
                    account_name=self.blob_service_client.account_name,
                    container_name=container_name,
                    blob_name=blob.name,
                    account_key=self.blob_service_client.credential.account_key,
                    permission=BlobSasPermissions(read=True),
                    expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
                )
                sas_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob.name}?{sas_blob}"
                powerpoint_sas_urls[blob.name] = sas_url

        return powerpoint_sas_urls
    
    def generate_excel_sas_urls(self, container_name, project, expiry_hours=24):
        """
        Generate SAS URLs for all Excel files in the specified container.
        """
        excel_sas_urls = {}
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=project)

        for blob in blob_list:
            if blob.name.lower().endswith((".xlsx", ".xls")):
                sas_blob = generate_blob_sas(
                    account_name=self.blob_service_client.account_name,
                    container_name=container_name,
                    blob_name=blob.name,
                    account_key=self.blob_service_client.credential.account_key,
                    permission=BlobSasPermissions(read=True),
                    expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
                )
                sas_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob.name}?{sas_blob}"
                excel_sas_urls[blob.name] = sas_url

        return excel_sas_urls
    
    def generate_zip_sas_urls(self, container_name, project, expiry_hours=24):
        """
        Generate SAS URLs for all ZIP files in the specified container.
        """
        zip_sas_urls = {}
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=project)

        for blob in blob_list:
            if blob.name.lower().endswith(".zip"):
                sas_blob = generate_blob_sas(
                    account_name=self.blob_service_client.account_name,
                    container_name=container_name,
                    blob_name=blob.name,
                    account_key=self.blob_service_client.credential.account_key,
                    permission=BlobSasPermissions(read=True),
                    expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
                )
                sas_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob.name}?{sas_blob}"
                zip_sas_urls[blob.name] = sas_url

        return zip_sas_urls
    
    def get_image_as_pil(self, container_name, blob_name):
        """
        Fetches an image from Azure Blob Storage and returns it as a PIL Image object.

        :param container_name: The name of the Azure Blob Storage container.
        :param blob_name: The name of the blob (image) in the container.
        :return: PIL Image object.
        """
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        stream = io.BytesIO(blob_client.download_blob().readall())
        return Image.open(stream)
    
    @staticmethod
    def upload_images_to_azure(images, container_name, connection_string, path=None):
        """
        Uploads images to an Azure Blob Storage container with unique UUIDs as filenames.
        If a path is provided, images are read from the path. Otherwise, images from the provided list are used.
        
        :param images: List of images (PIL Image objects or bytes). Used if path is None.
        :param container_name: The name of the Azure Blob Storage container.
        :param connection_string: Your Azure Storage Account connection string.
        :param path: Optional. Path to a directory containing images or a single image file.
        :return: List of filenames with UUIDs.
        """
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a container if it doesn't exist
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            container_client.create_container()

        uploaded_filenames = []

        # Process images from the path if provided
        if path:
            if os.path.isdir(path):
                images = [os.path.join(path, file) for file in os.listdir(path) if file.lower().endswith(('png', 'jpg', 'jpeg', 'gif', 'bmp'))]
            elif os.path.isfile(path):
                images = [path]
            else:
                raise ValueError("Provided path is neither a directory nor a file.")

        for img_path in images:
            if isinstance(img_path, str):
                # Open image from a file path
                image = Image.open(img_path)
            elif isinstance(img_path, bytes):
                # Open image from bytes
                image = Image.open(io.BytesIO(img_path))
            elif isinstance(img_path, Image.Image):
                # Use the image directly if it's already a PIL Image
                image = img_path
            else:    
                uploaded_filenames.append(None)
                continue


            # Handle RGBA to RGB conversion if necessary
            if image.mode == 'RGBA':
                image = image.convert('RGB')

            # Generate a unique filename using UUID
            filename = f"{uuid.uuid4()}.png"

            # Convert PIL Image to bytes
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_byte_arr = img_byte_arr.getvalue()

            # Create a blob client and upload the image
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
            blob_client.upload_blob(img_byte_arr, blob_type="BlockBlob")

            uploaded_filenames.append(filename)
            
        return uploaded_filenames
    
    @staticmethod
    def upload_image_to_azure(images, container_name, connection_string, path=None):
        """
        Uploads a single image to an Azure Blob Storage container with a unique UUID as the filename.
        If a path is provided, the image is read from the path. Otherwise, the provided image is used.
        
        :param image: A single image (PIL Image object or bytes). Used if path is None.
        :param container_name: The name of the Azure Blob Storage container.
        :param connection_string: Your Azure Storage Account connection string.
        :param path: Optional. Path to an image file.
        :return: Filename with UUID.
        """
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a container if it doesn't exist
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            container_client.create_container()

        # Process images from the path if provided
        if path:
            if os.path.isfile(path):
                image = Image.open(path)
            else:
                raise ValueError("Provided path is not a valid file.")
        
        if isinstance(images, str):
            # Open image from a file path
            image = Image.open(images)
        elif isinstance(images, bytes):
            # Open image from bytes
            image = Image.open(io.BytesIO(images))
        elif isinstance(images, Image.Image):
            # Use the image directly if it's already a PIL Image
            image = images
        else:
            return None


        # Handle RGBA to RGB conversion if necessary
        if image.mode == 'RGBA':
            image = image.convert('RGB')

        # Generate a unique filename using UUID
        filename = f"{uuid.uuid4()}.png"

        # Convert PIL Image to bytes
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='PNG')
        img_byte_arr = img_byte_arr.getvalue()

        # Create a blob client and upload the image
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
        blob_client.upload_blob(img_byte_arr, blob_type="BlockBlob")

        return filename
    
    @staticmethod
    def add_collaboration(box_client, folder_id, email, access_level='viewer'):
        """
        Add a collaboration to a Box folder.

        :param box_client: Box client object.
        :param folder_id: ID of the Box folder.
        :param email: Email of the user to collaborate with.
        :param access_level: Level of access (e.g., 'viewer', 'editor'). Default is 'viewer'.
        """
        try:
            folder = box_client.folder(folder_id=folder_id)
            collaboration = folder.add_collaborator(email, access_level)
            logger.info(f"Added collaboration: {collaboration.id} with {email} as {access_level}")
        except BoxAPIException as e:
            logger.error(f"Box API Exception during collaboration: {e}")
            raise

    @staticmethod
    def transfer_data_to_box(azure_connection_string, azure_container_name, box_folder_id, azure_folder_path=None, email_to_share = None):
        """
        Transfer data from an Azure container to a Box folder. 
        If a folder path is specified, upload that folder only; otherwise, upload the entire container.

        :param azure_connection_string: Connection string for Azure Blob Storage.
        :param azure_container_name: Name of the Azure container.
        :param box_folder_id: ID of the folder in Box where files will be uploaded.
        :param azure_folder_path: Path of the folder in Azure to upload (optional).
        """
        def sanitize_file_name(file_name):
            file_name = file_name.strip()
            file_name = re.sub(r'[\/\\:*?"<>|]', '_', file_name)
            max_length = 255
            if len(file_name) > max_length:
                file_ext = file_name.split('.')[-1] if '.' in file_name else ''
                file_name = file_name[:max_length - len(file_ext) - 1] + '.' + file_ext
            return file_name

        def create_folders(box_client, parent_id, folder_path):
            """
            Recursively create folders in Box according to the specified folder path.

            :param box_client: Box client object.
            :param parent_id: ID of the parent folder in Box.
            :param folder_path: Path of the folder to create.
            :return: ID of the final child folder.
            """
            if folder_path.startswith('/'):
                folder_path = folder_path[1:]  # Remove leading slash if present
            folder_names = folder_path.split('/')
            current_folder_id = parent_id
            for folder_name in folder_names:
                sanitized_folder_name = sanitize_file_name(folder_name)
                try:
                    items = box_client.folder(current_folder_id).get_items()
                    folder = next((item for item in items if item.type == 'folder' and item.name == sanitized_folder_name), None)

                    if folder is None:
                        folder = box_client.folder(current_folder_id).create_subfolder(sanitized_folder_name)
                        logger.info(f"Created folder '{sanitized_folder_name}' with ID: {folder.id}")
                    else:
                        logger.info(f"Folder '{sanitized_folder_name}' found with ID: {folder.id}")

                    current_folder_id = folder.id

                except BoxAPIException as e:
                    logger.error(f"Box API Exception: {e}")
                    logger.error(f"Status: {e.status}, Code: {e.code}, Message: {e.message}")
                    raise
                except Exception as e:
                    logger.error(f"General Exception: {e}")
                    raise
            return current_folder_id

        # Initialize Azure Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        container_client = blob_service_client.get_container_client(azure_container_name)

        # Initialize Box Client
        # Read the config file and parse it
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)
        auth = JWTAuth.from_settings_dictionary(config)
        box_client = Client(auth)
        # List all blobs in Azure container or specific folder
        blob_prefix = azure_folder_path if azure_folder_path else ''
        blobs = container_client.list_blobs(name_starts_with=blob_prefix)

        for blob in blobs:
            if not blob.name.endswith('/'):  # Skip directories
                # Separate the file name and its directory path
                dir_path, file_name = os.path.split(blob.name)
                final_folder_id = create_folders(box_client, box_folder_id, dir_path) if dir_path else box_folder_id
                box_folder = box_client.folder(folder_id=final_folder_id)

                # Download the blob
                blob_client = blob_service_client.get_blob_client(container=azure_container_name, blob=blob.name)
                stream = io.BytesIO(blob_client.download_blob().readall())

                # Check if file exists in Box and update or upload accordingly
                sanitized_name = sanitize_file_name(file_name)
                items = box_folder.get_items()
                existing_file = next((item for item in items if item.type == 'file' and item.name == sanitized_name), None)

                if existing_file:
                    # File exists, so update it
                    updated_file = existing_file.update_contents_with_stream(stream)
                    logger.info(f"Updated {sanitized_name} in Box folder {final_folder_id}")
                else:
                    # File does not exist, so upload it
                    box_folder.upload_stream(stream, sanitized_name)
                    logger.info(f"Uploaded {sanitized_name} to Box folder {final_folder_id}")
                    # Add collaboration to the folder
                    if email_to_share is not None:
                        AzureStorage.add_collaboration(box_client, final_folder_id, email_to_share, access_level='editor')  # Choose appropriate access level
                