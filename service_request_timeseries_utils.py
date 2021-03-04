import logging
import os
import tempfile

from db_utils import minio_utils

import pandas
import pytz

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_BUCKET_NAME = "service-standards-tool.citizen-notifications"
SAST_TZ = pytz.FixedOffset(120)
DATA_START_DATE = pandas.Timestamp("2020-03-01", tz=SAST_TZ)

DATE_COL = "Date"
OPENED_COL = "Opened"
CLOSED_COL = "Closed"

DURATION_COL = "Duration"

PREVIOUS_OPEN_COL = "PreviousOpened"
PREVIOUS_CLOSED_COL = "PreviousClosed"

WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"


def get_service_request_data(data_bucket, minio_access, minio_secret):
    sr_df = minio_utils.minio_to_dataframe(
        minio_bucket=data_bucket,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=minio_utils.DataClassification.LAKE,
        use_cache=True
    )

    return sr_df


def write_to_minio(html, minio_filename, minio_access, minio_secret):
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, minio_filename)

        logging.debug(f"Writing out HTML to '{local_path}'")
        with open(local_path, "w") as line_plot_file:
            line_plot_file.write(html)

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
