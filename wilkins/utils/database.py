import pandas as pd
import requests
import numpy as np
import json
from pandas.api.types import CategoricalDtype
from datetime import datetime
import logging
import hashlib

logger = logging.getLogger(__name__)


def generate_md5_as_number(row):
    # Check if the columns exist in the DataFrame
    media_type = (
        str(row.get("Media Type", "")) if not pd.isna(row.get("Media Type")) else ""
    )
    vendor = str(row.get("Vendor", "")) if not pd.isna(row.get("Vendor")) else ""
    town = str(row.get("Town ", "")) if not pd.isna(row.get("Town ")) else ""
    unit_number = str(row.get("Unit #", "")) if not pd.isna(row.get("Unit #")) else ""
    weekly_impressions = (
        str(row.get("A18+ Weekly Impressions", ""))
        if not pd.isna(row.get("A18+ Weekly Impressions"))
        else ""
    )

    data_string = f"{media_type}_{vendor}_{town}_{unit_number}_{weekly_impressions}"
    return hashlib.md5(data_string.encode()).hexdigest()


def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, "%m/%d/%y").date()
        return True
    except ValueError:
        return False


def convert_row_to_payload(row):
    payload = {}

    field_mapping = {
        "Vendor": "vendor",
        "Market": "market",
        "Zip": "state",
        "Town ": "town",
        "Media Type": "media_type",
        "# of Units ": "total_units",
        "TAB ID ": "geopath_id",
        "Unit #": "unit",
        "Location Description": "location_description",
        "Facing": "facing",
        'Size': 'size',
        "Availability": "availability_start",
        "A18+ Weekly Impressions": "a18_weekly_impressions",
        "A18+ Reach (%) ": "a18_4wk_reach",
        "A18+ Freq (x)": "a18_4wk_freq",
        "Illuminated?\n(Y or N)": "is_illuminated",
        "1 Week Media Cost": "one_week_media_cost",
        "2 Week Media Cost ": "two_week_media_cost",
        "3 Week Media Cost ": "three_week_media_cost",
        "4 Week Media Cost": "four_week_media_cost",
        "Installation Cost ": "installation_cost",
        "Production Cost": "production_cost",
        "Is Production Forced ": "is_prod_forced",
        "Target Location ": "target_location",
        "Distance to Location ": "distance_to_location",
        "Unit Highlights ": "unit_highlights",
        "Latitude": "latitude",
        "Longitude": "longitude",
        " Spot Length\n(Seconds)": "spot_length_secs",
        "# of Spots Per Loop ": "no_of_spots_per_loop",
        "Images":"image_id",
        "unit_id":"unit_id",
    }

    MAX_LENGTH = 255  # Maximum length for storing string values

    for df_column, schema_field in field_mapping.items():
        if df_column in row:
            value = row[df_column]

            if pd.notna(value):
                try:
                    # Remove dollar signs and percentage signs
                    value = str(value).replace("$", "").replace("%", "")

                    # Handle special cases
                    if "n/a - static" in value.lower():
                        value = None
                    elif schema_field in "location_description":
                        # Truncate the location description if it exceeds the maximum length
                        value = value[:MAX_LENGTH]
                    else:
                        # Explicitly cast the value to the expected type based on the schema
                        if schema_field in "unit":
                            # if pd.isna(value) or value.strip() == "":
                            #     value = generate_md5_as_number(row)
                            # else:
                            value = str(value)
                        elif (
                            schema_field in "availability_start"
                            or schema_field in "availability_end"
                        ):
                            dates = str(value).split("-")
                            start_date, end_date = None, None

                            if len(dates) == 2:
                                if is_valid_date(dates[0].strip()):
                                    start_date = (
                                        datetime.strptime(dates[0].strip(), "%m/%d/%y")
                                        .date()
                                        .isoformat()
                                    )
                                if is_valid_date(dates[1].strip()):
                                    end_date = (
                                        datetime.strptime(dates[1].strip(), "%m/%d/%y")
                                        .date()
                                        .isoformat()
                                    )

                            elif len(dates) == 1 and is_valid_date(dates[0].strip()):
                                start_date = (
                                    datetime.strptime(dates[0].strip(), "%m/%d/%y")
                                    .date()
                                    .isoformat()
                                )

                            payload["availability_start"] = start_date
                            payload["availability_end"] = end_date
                            continue
                        elif (
                            schema_field in "a18_weekly_impressions"
                            or schema_field in "total_units"
                        ):
                            try:
                                value = int(float(value.replace(",", "")))
                            except ValueError:
                                logger.error(
                                    f"Invalid integer value for column {df_column}: {value}"
                                )
                                value = None

                        elif (
                            schema_field in "a18_4wk_reach"
                            or schema_field in "a18_4wk_freq"
                            or schema_field in "installation_cost"
                            or schema_field in "one_week_media_cost"
                            or schema_field in "two_week_media_cost"
                            or schema_field in "three_week_media_cost"
                            or schema_field in "four_week_media_cost"
                            or schema_field in "production_cost"
                            or schema_field in "distance_to_location"
                            or schema_field in "no_of_spots_per_loop"
                            or schema_field in "spot_length_secs"
                            or schema_field in "latitude"
                            or schema_field in "longitude"
                        ):
                            value = float(value.replace(",", ""))
                            if not (-1e308 < value < 1e308):  # Adjust range as needed
                                value = None  # Replace invalid/out-of-range floats with None
                        elif (
                            schema_field in "is_illuminated"
                            or schema_field in "is_prod_forced"
                        ):
                            value = True if value.lower() in "yes" else False
                        elif schema_field in "image_id":
                            value = str(value)
                        else:
                            value = str(value[:MAX_LENGTH])
                except Exception as e:
                    logger.error(f"Error converting value for column {df_column}: {e}")
                    value = None

                payload[schema_field] = value
            else:
                # If value is NaN or NaT, set it to None
                payload[schema_field] = None
                if schema_field in "unit_id":
                    if pd.isna(value) or value.strip() == "":
                        payload[schema_field] = generate_md5_as_number(row)
        else:
            payload[schema_field] = None
            if df_column in "unit_id":
                payload[schema_field] = generate_md5_as_number(row)
    return payload


async def create_submission(df, headers, submission_url):
    for _, row in df.iterrows():
        submissions = convert_row_to_payload(row)
        # Manually replace special float values before JSON serialization
        json_data = json.dumps(submissions)

        submission_response = requests.post(
            submission_url, headers=headers, json=json.loads(json_data)
        )

        if submission_response.status_code == 201:
            logger.info("Submission created successfully!")
            logger.info("Response JSON:", submission_response.json())
        else:
            logger.error(
                "Failed to create submission.\n Status Code: %d Response Text: %s",
                submission_response.status_code,
                submission_response.text,
            )


def fetch_projects(BASE_URL, header):
    response = requests.get(f"{BASE_URL}/projects", headers=header)
    if response.status_code == 200:
        return response.json()["data"]
    return []


def create_project(BASE_URL, header, wilkins_id, name, client, status):
    response = requests.post(
        f"{BASE_URL}/projects",
        headers=header,
        json={
            "wilkins_id": wilkins_id,
            "name": name,
            "client": client,
            "status": status,
        },
    )
    logger.info(f"Base URL : {BASE_URL}, id: {wilkins_id}")
    if response.status_code == 201:
        logger.info("Project created successfully:", response.json())
    else:
        logger.error(f"Failed to create project: {response.status_code}, {response.text}")


def check_and_create_project(BASE_URL, header, wilkins_id, name, client, status):
    projects = fetch_projects(BASE_URL, header=header)
    if not any(project["wilkins_id"] == wilkins_id for project in projects):
        create_project(BASE_URL, header, wilkins_id, name, client, status)
    else:
        logger.info("Project already exists")


def fetch_vendors(BASE_URL, header):
    # Assuming this endpoint returns all vendors
    response = requests.get(BASE_URL + "/vendors", headers=header)
    if response.status_code == 200:
        return response.json()
    return []


def create_vendor(BASE_URL, header, wilkins_id, vendor_name):
    response = requests.post(
        BASE_URL + f"/projects/{wilkins_id}/vendors",
        headers=header,
        json={"name": vendor_name},
    )
    if response.status_code == 201:
        logger.info("Vendor created successfully:", response.json())
    else:
        logger.error(f"Failed to create vendor: {response.status_code}, {response.text}")


def check_and_create_vendor(BASE_URL, header, wilkins_id, vendor_name):
    vendors = fetch_vendors(BASE_URL, header)
    if not any(vendor == vendor_name for vendor in vendors):
        create_vendor(BASE_URL, header, wilkins_id, vendor_name)
    else:
        logger.info("Vendor already exists")


async def fetch_submissions(
    BASE_URL,
    header,
    wilkins_id,
    state=None,
    town=None,
    media_type=None,
    vendor=None,
    illuminated=None,
    selected=None,
    limit=0,
    skip=0,
):
    params = {
        "state": state,
        "town": town,
        "media_type": media_type,
        "vendor": vendor,
        "illuminated": illuminated,
        "selected": selected,
        # "limit": limit,
        # "skip": skip,
    }

    # Remove None values from params
    params = {k: v for k, v in params.items() if v is not None}

    response = requests.get(
        f"{BASE_URL}/projects/{wilkins_id}/submissions", headers=header, params=params
    )
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(
            "Failed to fetch submissions:", response.status_code, response.text
        )
        return None
