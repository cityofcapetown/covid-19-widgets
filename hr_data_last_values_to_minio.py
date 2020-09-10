import datetime
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
STAFF_NUMBER_COL_NAME = "StaffNumber"
DATE_COL_NAME = "Date"
STATUS_COL = "Categories"
SUCCINCT_STATUS_COL = "SuccinctStatus"
ASSESSED_COL = "AssessedStaff"

ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M"
STATUS_WINDOW_LENGTH = 4
TZ_STRING = "Africa/Johannesburg"

WORKING_STATUS = "working"
NOT_WORKING_STATUS = "not-working"
STATUSES_TO_SUCCINCT_MAP = {
    'At work': WORKING_STATUS,
    'Working from Home': WORKING_STATUS,
    'Off-Site': WORKING_STATUS,
    'COVID-19 Quarantine – Working': WORKING_STATUS,
    'COVID-19 Quarantine - Not Working': NOT_WORKING_STATUS,
    'COVID-19 Lockdown': WORKING_STATUS,
    'Sick Leave (linked to COVID-19)': NOT_WORKING_STATUS,
    'Sick Leave (NOT linked to COVID-19)': NOT_WORKING_STATUS,
    'Leave': NOT_WORKING_STATUS,
    'Suspended': NOT_WORKING_STATUS,
    'Absent': NOT_WORKING_STATUS
}
REMOTE_WORK_STATUSES = {
    'Working from Home', 'Off-Site', 'COVID-19 Quarantine – Working'
}
SICK_STATUSES = {
    'Sick Leave (linked to COVID-19)', 'Sick Leave (NOT linked to COVID-19)'
}
COVID_STATUSES = {
    'Sick Leave (linked to COVID-19)', 'COVID-19 Quarantine – Working', 'COVID-19 Quarantine - Not Working'
}

WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
OUTPUT_VALUE_FILENAME = "values.json"


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


def merge_df(hr_df, hr_master_df):
    combined_df = hr_df.merge(
        hr_master_df,
        left_on=STAFF_NUMBER_COL_NAME,
        right_on=STAFF_NUMBER_COL_NAME,
        how='left',
        validate="many_to_one",
    )
    logging.debug(f"combined_df.head(5)=\n{combined_df.head(5)}")

    return combined_df


def directorate_filter_df(hr_df, directorate_title, department_title=None):
    logging.debug(f"hr_df.shape={hr_df.shape}")
    filtered_df = (
        hr_df.query(
            f"Directorate.str.lower() == '{directorate_title.lower()}'"
        ) if directorate_title != "*" else hr_df
    )
    logging.debug(f"(post directorate filter) filtered_df.shape={filtered_df.shape}")

    filtered_df = (
        filtered_df.query(
            f"Department.str.lower() == '{department_title.lower()}'"
        ) if department_title else filtered_df
    )
    logging.debug(f"(post dept filter) filtered_df.shape={filtered_df.shape}")

    return filtered_df


def make_statuses_succinct_again(hr_df):
    hr_df[SUCCINCT_STATUS_COL] = hr_df[STATUS_COL].apply(STATUSES_TO_SUCCINCT_MAP.get)

    return hr_df


def get_current_hr_df(hr_df):
    most_recent_ts = hr_df[DATE_COL_NAME].max()
    logging.debug(f"most_recent_ts.date()={most_recent_ts.date()}")

    date_window_start = most_recent_ts.date() - datetime.timedelta(days=STATUS_WINDOW_LENGTH)
    logging.debug(f"date_window_start={date_window_start}")

    # select everyone inside the time window
    # then, sort, drop duplicates to only keep the most recent one
    current_hr_df = hr_df[
        hr_df[DATE_COL_NAME].dt.date >= date_window_start
        ].sort_values(
        by=[STAFF_NUMBER_COL_NAME, DATE_COL_NAME], ascending=False
    ).drop_duplicates(
        subset=[STAFF_NUMBER_COL_NAME]
    )

    return most_recent_ts, current_hr_df


def get_latest_values_dict(hr_df, hr_master_df, prefix="city"):
    most_recent_ts, current_hr_df = get_current_hr_df(hr_df)

    last_updated = most_recent_ts.strftime(ISO_TIMESTAMP_FORMAT)
    staff_reported_hr_form = (
        hr_master_df[STAFF_NUMBER_COL_NAME].isin(current_hr_df[STAFF_NUMBER_COL_NAME]) &
        hr_master_df[ASSESSED_COL]
    ).sum()
    staff_reported_sap = (
        hr_master_df[STAFF_NUMBER_COL_NAME].isin(current_hr_df[STAFF_NUMBER_COL_NAME]) &
        (~hr_master_df[ASSESSED_COL])
    ).sum()
    staff_reported = staff_reported_sap + staff_reported_hr_form

    staff_at_work = (current_hr_df[SUCCINCT_STATUS_COL] == WORKING_STATUS).sum() if staff_reported > 0 else 0
    staff_working_remotely = current_hr_df[STATUS_COL].isin(REMOTE_WORK_STATUSES).sum() if staff_reported > 0 else 0
    staff_sick = current_hr_df[STATUS_COL].isin(SICK_STATUSES).sum() if staff_reported > 0 else 0
    staff_covid = current_hr_df[STATUS_COL].isin(COVID_STATUSES).sum() if staff_reported > 0 else 0

    staff_assessed_hr_form = hr_master_df[ASSESSED_COL].sum()
    staff_assessed_sap = staff_reported_sap
    staff_assessed = staff_reported_hr_form + staff_assessed_sap

    staff_total = hr_master_df.shape[0]

    business_continuity_dict = {
        f"{prefix}_last_updated": last_updated,
        f"{prefix}_staff_at_work": str(staff_at_work),
        f"{prefix}_staff_reported": str(staff_reported),
        f"{prefix}_staff_reported_hr_form": str(staff_reported_hr_form),
        f"{prefix}_staff_reported_sap": str(staff_reported_sap),
        f"{prefix}_staff_working_remotely": str(staff_working_remotely),
        f"{prefix}_staff_sick": str(staff_sick),
        f"{prefix}_staff_covid": str(staff_covid),
        f"{prefix}_staff_assessed": str(staff_assessed),
        f"{prefix}_staff_assessed_hr_form": str(staff_assessed_hr_form),
        f"{prefix}_staff_assessed_sap": str(staff_assessed_sap),
        f"{prefix}_staff_total": str(staff_total)
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

    directorate_file_prefix = sys.argv[1]
    directorate_title = sys.argv[2]
    logging.debug(f"directorate_file_prefix={directorate_file_prefix}, directorate_title={directorate_title}")

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

    logging.info("Merg[ing] data...")
    hr_combined_df = merge_df(hr_transactional_data_df, hr_master_data_df)
    logging.info("...Merg[ed] data")

    logging.info("Filter[ing] data...")
    hr_filtered_df = directorate_filter_df(hr_combined_df, directorate_title)
    hr_filtered_master_df = directorate_filter_df(hr_master_data_df, directorate_title)
    logging.info("Filter[ed] data...")

    logging.info("Add[ing] succinct status column...")
    hr_transactional_data_df = make_statuses_succinct_again(hr_filtered_df)
    logging.info("...Add[ed] succinct status column.")

    logging.info("Generat[ing] latest values...")
    latest_values_dict = get_latest_values_dict(hr_filtered_df, hr_filtered_master_df, directorate_file_prefix)
    latest_values_json = to_json_data(latest_values_dict)
    logging.info("...Generat[ed] latest values")

    logging.info("Writ[ing] everything to Minio...")
    for content, filename in (
            (latest_values_json, f"{directorate_file_prefix}_{OUTPUT_VALUE_FILENAME}"),
    ):
        write_to_minio(content, filename,
                       secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")

    logging.info("...Done!")
