import json
import logging
import os
import pprint
import sys
import tempfile

from db_utils import minio_utils
import pandas

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_RESTRICTED_PREFIX = "data/private/"
HR_DATA_FILENAME = "business_continuity_people_status.csv"
HR_MASTER_DATA_FILENAME = "city_people.csv"
DATE_COL_NAME = "Date"
STATUS_COL = "Categories"
SUCCINCT_STATUS_COL = "SuccinctStatus"
ESSENTIAL_COL = "EssentialStaff"

TZ_STRING = "Africa/Johannesburg"
ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M"

WORKING_STATUS = "working"
NOT_WORKING_STATUS = "not-working"
STATUSES_TO_SUCCINCT_MAP = {
    "Working remotely (NO Covid-19 exposure)": WORKING_STATUS,
    "Working remotely (NO COVID 19 exposure)": WORKING_STATUS,
    "At work (on site)": WORKING_STATUS,
    "On leave": NOT_WORKING_STATUS,
    "On suspension": NOT_WORKING_STATUS,
    "Absent from work (unauthorised)": NOT_WORKING_STATUS,
    "Quarantine leave – working remotely": WORKING_STATUS,
    "Quarantine leave – unable to work remotely": NOT_WORKING_STATUS,
    "Quarantine leave – working remotely, Covid-19 exposure / isolation": WORKING_STATUS,
    "Quarantine leave – working remotely, COVID 19 exposure / isolation": WORKING_STATUS,
    "Sick (linked to Covid-19)": NOT_WORKING_STATUS,
    "Sick (linked to COVID 19)": NOT_WORKING_STATUS,
    "Sick (NOT linked to Covid-19)": NOT_WORKING_STATUS,
    "Sick (NOT linked to COVID 19)": NOT_WORKING_STATUS,
    "On Lockdown leave – unable to work remotely": NOT_WORKING_STATUS,
    "On Lockdown leave – able to work remotely": NOT_WORKING_STATUS
}
REMOTE_WORK_STATUSES = {
    "Working remotely (NO Covid-19 exposure)",
    "Working remotely (NO COVID 19 exposure)",
    "Quarantine leave – working remotely",
    "Quarantine leave – working remotely, Covid-19 exposure / isolation",
    "Quarantine leave – working remotely, Covid-19 exposure / isolation",
}
SICK_STATUSES = {
    "Sick (linked to Covid-19)",
    "Sick (linked to COVID 19)",
    "Sick (NOT linked to Covid-19)",
    "Sick (NOT linked to COVID 19)"
}
COVID_STATUSES = {
    "Sick (linked to Covid-19)",
    "Sick (linked to COVID 19)",
    "Quarantine leave – working remotely, Covid-19 exposure / isolation",
    "Quarantine leave – working remotely, COVID 19 exposure / isolation",
}

WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
OUTPUT_VALUE_FILENAME = "values_v2.json"


def get_data(minio_key, minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_datafile:
        minio_utils.minio_to_file(
            filename=temp_datafile.name,
            minio_filename_override=DATA_RESTRICTED_PREFIX + minio_key,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        data_df = pandas.read_csv(temp_datafile.name)

    return data_df


def make_statuses_succinct_again(hr_df):
    hr_df[SUCCINCT_STATUS_COL] = hr_df[STATUS_COL].apply(STATUSES_TO_SUCCINCT_MAP.get)

    return hr_df


def get_latest_values_dict(hr_df, hr_master_df):
    most_recent_date = hr_df[DATE_COL_NAME].max()
    current_hr_df = hr_df[
        hr_df[DATE_COL_NAME].dt.date == most_recent_date.date()
        ]
    logging.debug(f"most_recent_date.date()={most_recent_date.date()}")

    last_updated = most_recent_date.strftime(ISO_TIMESTAMP_FORMAT)
    staff_reported = current_hr_df.shape[0]

    staff_at_work = (current_hr_df[SUCCINCT_STATUS_COL] == WORKING_STATUS).sum() if staff_reported > 0 else 0
    staff_working_remotely = current_hr_df[STATUS_COL].isin(REMOTE_WORK_STATUSES).sum() if staff_reported > 0 else 0
    staff_sick = current_hr_df[STATUS_COL].isin(SICK_STATUSES).sum() if staff_reported > 0 else 0
    staff_covid = current_hr_df[STATUS_COL].isin(COVID_STATUSES).sum() if staff_reported > 0 else 0

    staff_essential = hr_master_df[ESSENTIAL_COL].sum()

    business_continuity_dict = {
        "last_updated": last_updated,
        "staff_at_work": str(staff_at_work),
        "staff_reported": str(staff_reported),
        "staff_working_remotely": str(staff_working_remotely),
        "staff_sick": str(staff_sick),
        "staff_covid": str(staff_covid),
        "staff_essential": str(staff_essential)
    }
    logging.debug(f"business_continuity_dict=\n{pprint.pformat(business_continuity_dict)}")

    return business_continuity_dict


def to_json_data(values_dict):
    json_data = json.dumps(values_dict)

    return json_data


def write_to_minio(data, minio_filename, minio_access, minio_secret):
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, minio_filename)

        logging.debug(f"Writing out data to '{local_path}'")
        with open(local_path, "w") as line_plot_file:
            line_plot_file.write(data)

        logging.debug(f"Uploading '{local_path}' to Minio")
        result = minio_utils.file_to_minio(
            filename=local_path,
            filename_prefix_override=WIDGETS_RESTRICTED_PREFIX,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        assert result


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("Fetch[ing] data...")
    hr_transactional_data_df = get_data(HR_DATA_FILENAME,
                                        secrets["minio"]["edge"]["access"],
                                        secrets["minio"]["edge"]["secret"])
    hr_transactional_data_df[DATE_COL_NAME] = pandas.to_datetime(hr_transactional_data_df[DATE_COL_NAME])
    logging.debug(f"data_df.columns=\n{hr_transactional_data_df.columns}")
    logging.debug(
        f"data_df.columns=\n{pprint.pformat(hr_transactional_data_df.dtypes.to_dict())}"
    )

    hr_master_data_df = get_data(HR_MASTER_DATA_FILENAME,
                                 secrets["minio"]["edge"]["access"],
                                 secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")

    logging.info("Add[ing] succinct status column...")
    hr_transactional_data_df = make_statuses_succinct_again(hr_transactional_data_df)
    logging.info("...Add[ed] succinct status column.")

    logging.info("Generat[ing] latest values...")
    latest_values_dict = get_latest_values_dict(hr_transactional_data_df, hr_master_data_df)
    latest_values_json = to_json_data(latest_values_dict)
    logging.info("...Generat[ed] latest values")

    logging.info("Writ[ing] everything to Minio...")
    for content, filename in (
            (latest_values_json, OUTPUT_VALUE_FILENAME),
    ):
        write_to_minio(content, filename,
                       secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")

    logging.info("...Done!")
