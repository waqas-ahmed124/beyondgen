import logging
import logging.config
import os
from tempfile import TemporaryDirectory
import pandas as pd
import asyncio

import azure.functions as func
from wilkins.tools.azure import AzureStorage
from wilkins.project import Project
from wilkins.utils.database import fetch_submissions
from wilkins.utils.auth import get_access_token

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

INPUT_CONTAINER = "attachments"
IMAGES_CONTAINER = "images"
OUTPUT_CONTAINER = "results"
LOCAL_WORKDIR = "./output"
TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "wilkins", "data", "template.pptx"
)

# log_conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.conf")
# logging.config.fileConfig(log_conf_file)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    project = req.params.get("project")
    if not project:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            project = req_body.get("project")

    if project:
        try:
            access_token = get_access_token()
        except Exception as e:
            logging.error(f"Unable to get access-token for CLI: {e}")
            return func.HttpResponse(f"Unable to get access-token for CLI: {e}")

        with TemporaryDirectory() as temp_dir:
            working_dir = os.path.join(temp_dir, "output")
            logging.info(f"Current Working Directory : {working_dir}")
            os.makedirs(working_dir, exist_ok=True)
            p = Project.create_from_azure_container(INPUT_CONTAINER, project, access_token, working_dir)
            presentation_path = os.path.join(working_dir, "output.pptx")
            p.process_data(os.getenv("AZURE_STORAGE_CONNECTION_STRING"),container_name=IMAGES_CONTAINER)
            base_url = os.getenv("BASE_URL")
            # access_token = os.getenv("ACCESS_TOKEN")
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            response = asyncio.run(fetch_submissions(base_url, headers, project))
            azure_storage = AzureStorage(
                connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            )
            p.create_ppt(pd.DataFrame(response['data']),presentation_path,azure_storage=azure_storage,container_name=IMAGES_CONTAINER, template_path=TEMPLATE_PATH)
            azure_storage.upload_file_to_folder(
                OUTPUT_CONTAINER, project, presentation_path
            )
            result = azure_storage.generate_powerpoint_sas_urls(
                OUTPUT_CONTAINER, project
            )

            # TODO error handling
        return func.HttpResponse(f"{list(result.values())[0]}")

    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a project in the query string or in the request body for a personalized response.",
            status_code=200,
        )
    
@app.blob_trigger(path=INPUT_CONTAINER + "/{project}/{name}", connection="AzureWebJobsStorage", arg_name="blob")
def blob_trigger(blob: func.InputStream):
    logging.info(f"Blob trigger function processed blob\n"
                 f"Name: {blob.name}\n"
                 f"Blob Size: {blob.length} bytes")
    project_name = blob.name.split('/')[1]

    try:
        access_token = get_access_token()
    except Exception as e:
        logging.error(f"Unable to get access-token for CLI: {e}")
        return

    with TemporaryDirectory() as temp_dir:
            working_dir = os.path.join(temp_dir, "output")
            logging.info(f"Current Working Directory : {working_dir}")
            os.makedirs(working_dir, exist_ok=True)
            p = Project.create_from_azure_container(INPUT_CONTAINER, project_name, access_token, working_dir)
            p.process_data(os.getenv("AZURE_STORAGE_CONNECTION_STRING"),container_name=IMAGES_CONTAINER)

def generate_file_sas_url(file_type, project):
    azure_storage = AzureStorage(connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"))

    if file_type.lower() == "ppt":
        result = azure_storage.generate_powerpoint_sas_urls(OUTPUT_CONTAINER, project)
    elif file_type.lower() == "excel":
        result = azure_storage.generate_excel_sas_urls(OUTPUT_CONTAINER, project)
    elif file_type.lower() == "zip":
        result = azure_storage.generate_zip_sas_urls(OUTPUT_CONTAINER, project)
    else:
        raise ValueError("Invalid file type. Expected 'ppt', 'excel', or 'zip'.")

    return result

@app.route(route="generate_sas_url")
def generate_sas_url(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Generate SAS URL function processed a request.")

    project = req.params.get("project")
    file_type = req.params.get("file_type")

    if not project or not file_type:
        return func.HttpResponse(
            "Please specify both 'project' and 'file_type' ('ppt', 'excel', or 'zip') in the query string.",
            status_code=400
        )

    try:
        access_token = get_access_token()
    except Exception as e:
        logging.error(f"Unable to get access-token for CLI: {e}")
        return func.HttpResponse(f"Unable to get access-token for CLI: {e}")

    with TemporaryDirectory() as temp_dir:
            working_dir = os.path.join(temp_dir, "output")
            logging.info(f"Current Working Directory : {working_dir}")
            os.makedirs(working_dir, exist_ok=True)
            p = Project.create_from_azure_container(INPUT_CONTAINER, project, access_token, working_dir)
            presentation_path = os.path.join(working_dir, "output.pptx")
            excel_path = os.path.join(working_dir, "output.xlsx")
            zip_path = os.path.join(working_dir, "combined_files.zip")
            base_url = os.getenv("BASE_URL")
            # access_token = os.getenv("ACCESS_TOKEN")
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            response = asyncio.run(fetch_submissions(base_url, headers, project, selected=True))
            azure_storage = AzureStorage(
                connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            )
            if file_type.lower() == "ppt":
                p.create_ppt(pd.DataFrame(response['data']),presentation_path,azure_storage=azure_storage,container_name=IMAGES_CONTAINER, template_path=TEMPLATE_PATH)
                azure_storage.upload_file_to_folder(
                    OUTPUT_CONTAINER, project, presentation_path
                )
                result = azure_storage.generate_powerpoint_sas_urls(OUTPUT_CONTAINER, project)
                return func.HttpResponse(f"{list(result.values())[0]}")
            elif file_type.lower() == "excel":
                p.create_excel(pd.DataFrame(response['data']),excel_path)
                azure_storage.upload_file_to_folder(
                    OUTPUT_CONTAINER, project, excel_path
                )
                result = azure_storage.generate_excel_sas_urls(OUTPUT_CONTAINER, project)
                return func.HttpResponse(f"{list(result.values())[0]}")
            elif file_type.lower() == "zip":
                p.create_ppt(pd.DataFrame(response['data']),presentation_path,azure_storage=azure_storage,container_name=IMAGES_CONTAINER, template_path=TEMPLATE_PATH)
                p.create_excel(pd.DataFrame(response['data']),excel_path)
                p.zip_files(presentation_path,excel_path, zip_path)
                azure_storage.upload_file_to_folder(
                    OUTPUT_CONTAINER, project, zip_path
                )
                result = azure_storage.generate_zip_sas_urls(OUTPUT_CONTAINER, project)
                return func.HttpResponse(f"{list(result.values())[0]}")
            else:
                raise ValueError("Invalid file type. Expected 'ppt', 'excel', or 'zip'.")