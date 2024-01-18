import os
import logging
import logging.config
from dotenv import load_dotenv
import argparse

from wilkins.project import Project

load_dotenv()

INPUT_CONTAINER = "attachments"
OUTPUT_CONTAINER = "results"
LOCAL_WORKDIR = "./output"
PROJECT = "project_3883"
ACCESS_TOKEN = ""
TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "template.pptx"
)


log_conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.conf")
logging.config.fileConfig(log_conf_file)


def main(unit_number=None):
    p = Project.create_from_azure_container(
        INPUT_CONTAINER,
        PROJECT,
        ACCESS_TOKEN,
        LOCAL_WORKDIR,
        os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
    )
    presnetation_path = os.path.join(LOCAL_WORKDIR, "output.pptx")
    p.generate_powerpoint(
        presnetation_path, template_path=TEMPLATE_PATH, unit_number=unit_number
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate PowerPoint presentation")
    parser.add_argument("--unit-number", type=str, help="Unit number for processing")
    args = parser.parse_args()
    main(args.unit_number)
