import pandas as pd
import numpy as np
from typing import List
import re
import logging

logger = logging.getLogger(__name__)


def check_all_values_same_for_each_category(
    dataframe, category_column, target_column, target_value
):
    if (
        category_column not in dataframe.columns
        or target_column not in dataframe.columns
    ):
        raise ValueError("Specified column not found in DataFrame.")

    results = {}
    for category in dataframe[category_column].unique():
        filtered_df = dataframe[dataframe[category_column] == category]
        all_match = (filtered_df[target_column] == target_value).all()
        results[category] = all_match

        if not all_match:
            mismatching_values = filtered_df[
                filtered_df[target_column] != target_value
            ][target_column]
            logger.info(f"Mismatching values in category '{category}':")
            logger.info(mismatching_values.to_string(index=False))

    return results


def _find_max_matching_pdf(row, page_content):
    key_cols = ["Unit #", "Latitude", "Longitude", "Size"]

    # Function to remove text within brackets
    def remove_brackets(text):
        return re.sub(r"\(.*?\)", "", text).strip()

    # Apply the function to the "Unit #" value if it exists in the row
    if "Unit #" in row:
        row["Unit #"] = remove_brackets(str(row["Unit #"]))

    values = [row[col] for col in key_cols]
    return_value = sum([1 for value in values if str(value) in page_content])
    return return_value


def match_with_pdf_content(submission: pd.DataFrame, pdf_content, unit_number=None):
    if unit_number is not None:
        # Search for the specific unit number directly
        matching_rows = submission[submission["Unit #"] == unit_number]

        if not matching_rows.empty:
            # Mark the matching row(s) as image_matched
            submission.loc[matching_rows.index, "image_matched"] = True
            submission.loc[
                matching_rows.index, "match_image_index"
            ] = matching_rows.index

    # pages_already_matched = set()

    for row_idx, row in submission.iterrows():
        if row.image_matched:
            continue

        scores = np.zeros((len(pdf_content)))
        for idx, content in enumerate(pdf_content):
            # if idx in pages_already_matched:
            #     continue
            scores[idx] = _find_max_matching_pdf(row, content)

        logger.info("Matching score: %s", scores)
        # Check if scores array is empty or all zeros
        if np.any(scores):  # This will be False if scores is all zeros
            matched_idx = np.argmax(scores)
            # Only a non-zero match scores qualify as a match
            if scores[matched_idx] > 0:
                # pages_already_matched.add(matched_idx)
                submission.at[row_idx, "image_matched"] = True
                submission.at[row_idx, "match_image_index"] = matched_idx


def match_with_image_files(submission: pd.DataFrame, filenames):
    """As filenames correspond to the Unit #, we are directly matching the filenames and sending the indexes for images"""
    filenames_already_matched = set()
    for row_idx, row in submission.iterrows():
        unit_number = str(row["Unit #"]).strip().lower()
        media_type = str(row["Media Type"]).strip().lower()
        for idx, filename in enumerate(filenames):
            # if filename in filenames_already_matched:
            #     continue
            cleaned_filename = filename.strip().lower()
            if unit_number in cleaned_filename or media_type in cleaned_filename:
                filenames_already_matched.add(filename)
                submission.at[row_idx, "image_matched"] = True
                submission.at[row_idx, "match_image_file_index"] = idx
                break


def match_mediatype_with_pdf_content(submission: pd.DataFrame, pdf_content):
    for row_idx, row in submission.iterrows():
        # Normalize the mediatype for comparison
        mediatype_normalized = row["Media Type"].lower()
        if "bus" in row["Media Type"].lower():
            break
        for idx, content in enumerate(pdf_content):
            content_normalized = content.lower()

            # Check various normalized versions of the mediatype against the content
            if (
                mediatype_normalized in content_normalized
                or mediatype_normalized.rstrip("s") in content_normalized
                or mediatype_normalized.rstrip("es") in content_normalized
            ):
                submission.at[row_idx, "image_matched"] = True
                submission.at[row_idx, "match_image_index"] = idx
                break
            else:
                # If no match, check for '&' and '@', and split
                if "&" in mediatype_normalized or "@" in mediatype_normalized:
                    parts = re.split(r"[&@]", mediatype_normalized)
                    for part in parts:
                        part = part.strip()
                        if (
                            part in content_normalized
                            or part.rstrip("s") in content_normalized
                            or part.rstrip("es") in content_normalized
                        ):
                            submission.at[row_idx, "image_matched"] = True
                            submission.at[row_idx, "match_image_index"] = idx
                            break
                    if submission.at[row_idx, "image_matched"]:
                        break


def match_media_type(submission: pd.DataFrame, pdf_content):
    # Check vendors
    image_matched_value = False
    vendors_to_check = check_all_values_same_for_each_category(
        submission, "Vendor", "image_matched", image_matched_value
    )
    logger.info("Vendors To Check for media type matching : ", vendors_to_check)
    # Process each vendor
    for vendor, all_false in vendors_to_check.items():
        if all_false:
            vendor_df = submission[submission["Vendor"] == vendor]
            match_mediatype_with_pdf_content(vendor_df, pdf_content)
            # Update the original DataFrame
            submission.update(vendor_df)


def match_bus_media(submission: pd.DataFrame):
    image_matched_value = False
    vendors_to_check = check_all_values_same_for_each_category(
        submission, "Vendor", "image_matched", image_matched_value
    )
    for vendor, all_false in vendors_to_check.items():
        if all_false:
            # Check if its a bus media type
            vendor_df = submission[submission["Vendor"] == vendor]

            contains_bus = (
                vendor_df["Media Type"].str.contains("bus", case=False, na=False).any()
            )

            # If 'Bus' is found in any row, set all values in 'bus_media' column to 1
            if contains_bus:
                vendor_df["bus_media"] = 1
                submission.update(vendor_df)
