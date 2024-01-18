from wilkins.matching import (
    match_with_pdf_content,
    match_with_image_files,
    match_media_type,
    match_bus_media,
)
import asyncio
from wilkins.utils.images import extract_images_and_filenames
from wilkins.utils.dropbox import download_and_extract_zip
from wilkins.utils.msg import fetch_files_and_extract_links, classify_all_links
from wilkins.utils.epitch import (
    extract_epitch_content_and_images,
    extract_barrett_content_and_images,
)
from wilkins.utils.ppt import PowerPointCreator
from wilkins.utils.vision import filter_media, filter_media_bus
from wilkins.utils.pdf import extract_pdf_content_and_images, download_pdfs
from wilkins.utils.excel import (
    detect_sheet_and_header,
    clean_up,
    extract_images_to_dict,
    append_images_to_df,
)
from wilkins.tools.azure import AzureStorage
from wilkins.utils.gdrive import download_file_from_google_drive
from wilkins.utils.database import (
    check_and_create_project,
    check_and_create_vendor,
    create_submission,
)
import os
from typing import Optional, List
import logging
import glob
import pandas as pd
import random
import requests
from dotenv import load_dotenv
import shutil
import zipfile
import tempfile
load_dotenv()
logger = logging.getLogger(__name__)


async def load_image(image_index, media_images, image_data):
    if image_index != -1:
        return media_images[image_index]
    else:
        return None


async def load_images(submissions, media_images, image_data):
    submission_images = []
    tasks = []

    for _, row in submissions.iterrows():
        task = asyncio.create_task(
            load_image(row.match_image_file_index, media_images, image_data)
        )
        tasks.append(task)

    submission_images = await asyncio.gather(*tasks)

    return submission_images


class Project:
    @staticmethod
    def create_from_local_path(local_path: str) -> "Project":
        pass

    @staticmethod
    def create_from_azure_container(
        input_container_path: str,
        project_path: str,
        access_token: str,
        working_dir: str = "./workdir",
        az_connection_string: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
    ) -> "Project":
        azure_blob_handler = AzureStorage(az_connection_string)
        attachment_dir = os.path.join(working_dir, "submissions")
        # Fetches all files from the folders in the container
        azure_blob_handler.fetch_files_from_folder(
            input_container_path, project_path, attachment_dir
        )

        return Project(working_dir, project_path, access_token)

    def __init__(self, submissions_path: str, project_path: str, access_token: str) -> None:
        self.files = []
        self.submissions: pd.DataFrame = None
        self.submissions_path = submissions_path
        self.wilkins_id = project_path
        self.access_token = access_token
        # Upload the final submission to the database
        # access_token = os.getenv("ACCESS_TOKEN")
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        check_and_create_project(
            os.getenv("BASE_URL"),
            headers,
            project_path,
            name="",
            status="Active",
            client="",
        )
        self._process_vendor_submissions()

    def _process_vendor_submissions(self) -> pd.DataFrame:
        xlsx_files = glob.glob(
            os.path.join(self.submissions_path, "**", "*.xlsx"), recursive=True
        )

        all_dfs = []
        for file in xlsx_files:
            logger.info(f"Processing {file} for vendor submissions")
            target_sheets, header_rows = detect_sheet_and_header(file)

            logger.debug(f"{file}\n has {len(target_sheets)} sheets.")

            if not target_sheets:
                logger.info(f"Warning: No suitable sheet found in {file}. Skipping.")
                continue

            # Extract all images from the .xlsx file
            all_images = extract_images_to_dict(file)
            # Call append_images_to_df to get a dictionary of DataFrames
            data_dict = append_images_to_df(
                file, target_sheets, header_rows, all_images
            )

            # Extract the DataFrames from the dictionary and append them to all_df
            data_dfs = list(data_dict.values())
            all_dfs.extend(data_dfs)

        self.submissions = pd.concat(all_dfs, axis=0, ignore_index=True)
        if (
            "Unit #" in self.submissions.columns
            and self.submissions["Unit #"].notnull().any()
        ):
            self.submissions = self.submissions.drop_duplicates(subset=["Unit #"])
        else:
            self.submissions = self.submissions.drop_duplicates()

        self.submissions = self.submissions.assign(image_matched=False)
        self.submissions = self.submissions.assign(match_image_index=-1)
        logger.info(f"Total combined submissions: {self.submissions.shape[0]}")
        # Upload the final submission to the database
        # access_token = os.getenv("ACCESS_TOKEN")
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        check_and_create_vendor(
            os.getenv("BASE_URL"),
            headers,
            self.wilkins_id,
            self.submissions["Vendor"][0],
        )

    def process_data(
        self, Azure_Connection_String,container_name="images", unit_number=None
    ):
        all_links = fetch_files_and_extract_links(self.submissions_path)
        classified_links_dict = classify_all_links(all_links)
        for file_path, links in classified_links_dict.items():
            logger.info(
                f"Links : pdf - {links['pdf_links']} epitch - {links['epitch_links']} dropbox - {links['dropbox_links']} barrett - {links['barrett_links']}"
            )

            if len(links["pdf_links"]) != 0:
                # process pdf links
                download_pdfs(links["pdf_links"], self.submissions_path)

            if len(links["dropbox_links"]) != 0:
                # process dropbox links
                for link in links["dropbox_links"]:
                    download_and_extract_zip(link, self.submissions_path)

            if len(links["google_drive_links"]) != 0:
                for link in links["google_drive_links"]:
                    download_file_from_google_drive(link, self.submissions_path)

        pdf_data = extract_pdf_content_and_images(self.submissions_path)
        # Processing Epitch Links
        media_images = filter_media(pdf_data["images"])
        bus_images = filter_media_bus(pdf_data["images"])
        # For Epitches and Barrett Links, not filtering images and directly pushing
        for file_path, links in classified_links_dict.items():
            if len(links["epitch_links"]) != 0:
                # process epitch links
                logger.info("Processing Epitches....")
                for link in links["epitch_links"]:
                    epitch_data = extract_epitch_content_and_images(link)
                    # Append epitch data to pdf data
                    for key in epitch_data:
                        pdf_data[key].extend(epitch_data[key])
                    for group in epitch_data["images"]:
                        # This will add each image to media_images
                        media_images.extend(group)
            if len(links["barrett_links"]) != 0:
                # process barrett links
                logger.info("Processing Barrett Links....")
                barret_data = extract_barrett_content_and_images(links["barrett_links"])
                # Append barrett data to pdf data
                for key in barret_data:
                    pdf_data[key].extend(barret_data[key])
                media_images.extend(barret_data["images"])

        match_with_pdf_content(self.submissions, pdf_data["content"], unit_number)
        logger.info("Done with matching content")
        image_data = extract_images_and_filenames(self.submissions_path)
        self.submissions["match_image_file_index"] = -1
        match_with_image_files(self.submissions, image_data["content"])
        logger.info("Done with matching image files")
        # IFF no matches with Unit# for an entire vendor, match using media type
        match_media_type(self.submissions, pdf_data["content"])
        self.submissions["bus_media"] = -1
        match_bus_media(self.submissions)
        submission_images = []
        for _, row in self.submissions.iterrows():
            if row.match_image_index != -1:
                submission_images.append(media_images[row.match_image_index])
            elif row.match_image_file_index != -1:
                submission_images.append(
                    image_data["images"][row.match_image_file_index]
                )
            elif row.bus_media != -1:
                submission_images.append(random.choice(bus_images))
            else:
                submission_images.append(None)
        final_submission = clean_up(self.submissions)
        # Upload the final submission to the database
        # access_token = os.getenv("ACCESS_TOKEN")
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        uploaded_filenames1 = AzureStorage.upload_images_to_azure(submission_images,container_name,Azure_Connection_String)    
        images_to_upload = []
        # Collect images from rows
        for idx, row in final_submission.iterrows():
            if isinstance(row["Images"], bytes):
                images_to_upload.append(row["Images"])

        # Upload images and get filenames
        uploaded_filenames2 = AzureStorage.upload_images_to_azure(images_to_upload, container_name,Azure_Connection_String)

        # Replace image data in rows with filenames
        for idx, filename in enumerate(uploaded_filenames1):
            final_submission.loc[idx, "Images"] = filename
        for idx, filename in enumerate(uploaded_filenames2):
            final_submission.loc[idx, "Images"] = filename
        submission_endpoint = f"/projects/{self.wilkins_id}/submissions"  # Replace with your actual project ID
        base_url = os.getenv("BASE_URL")
        submission_url = f"{base_url}{submission_endpoint}"
        # Perform asynchronous image loading
        # loop = asyncio.get_event_loop()
        # submission_images = loop.run_until_complete(load_images(self.submissions, media_images, image_data))
        asyncio.run(create_submission(final_submission, headers, submission_url))
        # return final_submission

    def create_ppt(self, submission_data, output_file_path,azure_storage, container_name = "images", template_path="template.pptx"):
        ppt = PowerPointCreator(submission_data, template_path)
        ppt.create_presentation(output_file_path,azure_storage,container_name)

    def create_excel(self, submission_data, output_file_path):
        # Check if the directory of the output file exists, if not, create it
        output_dir = os.path.dirname(output_file_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Write the DataFrame to an Excel file
        submission_data.to_excel(output_file_path)
        logger.info(f'Excel file created at {output_file_path}')
    
    def zip_files(self, file1_path, file2_path, output_zip_path):
        # Check if the output directory exists, if not create it
        output_dir = os.path.dirname(output_zip_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Check and copy each file into the temporary directory
            for file_path in [file1_path, file2_path]:
                if os.path.exists(file_path):
                    shutil.copy(file_path, temp_dir)
                else:
                    logger.warning(f"Warning: {file_path} does not exist and will not be included in the zip.")

            # Zip the temporary folder
            with zipfile.ZipFile(output_zip_path, 'w') as zipf:
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        zipf.write(file_path, os.path.relpath(file_path, temp_dir))

        logger.info(f'Files zipped at {output_zip_path}')