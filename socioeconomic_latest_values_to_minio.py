import json
import pprint
import logging
import os
import sys

import pandas

from hr_data_last_values_to_minio import get_data, to_json_data, write_to_minio

SE_DATA_FILENAME = "socioeconomic_tidy_data.csv"

DATE_FORMAT = "%Y"

DATE_COL = "date"
MEASURE_COL = "measure"
FEATURE_COL = "feature"
VALUE_COL = "value"

SKIP_LIST = ["Formal job losses", "HH income change"]

METRICS_FIELD = "metrics"
METRICS_DELTA_FIELD = "metrics-delta"
METRICS_DELTA_RELATIVE_FIELD = "metrics-delta-relative"

SE_PREFIX = "city_socioeconomic"
OUTPUT_VALUE_FILENAME = f"{SE_PREFIX}_values.json"


def create_latest_se_values_dict(ts_df):
    # Removing those measures not included in the JSON
    ts_df = ts_df.query(f"~{MEASURE_COL}.isin(@SKIP_LIST)")

    latest_values_df = ts_df.sort_values(by=[FEATURE_COL, MEASURE_COL, DATE_COL]).drop_duplicates(
        subset=[FEATURE_COL, MEASURE_COL], keep="last"
    )
    latest_values_dict = latest_values_df.reset_index().to_dict(orient="records")

    logging.debug(f"latest_values_dict=\n{pprint.pformat(latest_values_dict)}")

    ref_values_dict = ts_df.drop(latest_values_df.index).sort_values(
        by=[FEATURE_COL, MEASURE_COL, DATE_COL]).drop_duplicates(
        subset=[FEATURE_COL, MEASURE_COL], keep="last"
    ).reset_index().to_dict(orient="records")

    logging.debug(f"ref_values_dict=\n{pprint.pformat(latest_values_dict)}")

    # transforming from flat records into heirachy of values
    output_dict = {}
    for value_dict, ref_value_dict in zip(latest_values_dict, ref_values_dict):
        assert value_dict[FEATURE_COL] == ref_value_dict[FEATURE_COL]
        assert value_dict[MEASURE_COL] == ref_value_dict[MEASURE_COL]

        feature_value = value_dict[FEATURE_COL]
        measure_value = value_dict[MEASURE_COL]

        # Creating the relevant location in the output hierachy using the measure
        value_output_dict = output_dict

        if measure_value not in value_output_dict:
            value_output_dict[measure_value] = {}
            logging.debug(f"Adding {measure_value} to values dict")

        value_output_dict[measure_value][feature_value] = {
            METRICS_FIELD: {DATE_COL: value_dict[DATE_COL].strftime("%Y")},
            METRICS_DELTA_FIELD: {DATE_COL: ref_value_dict[DATE_COL].strftime("%Y")},
        }

        output_value_dict = value_output_dict[measure_value][feature_value]

        output_value_dict[METRICS_FIELD][VALUE_COL] = value_dict[VALUE_COL]
        output_value_dict[METRICS_DELTA_FIELD][VALUE_COL] = (
                value_dict[VALUE_COL] - ref_value_dict[VALUE_COL]
        )

    logging.debug(f"output_dict=\n{pprint.pformat(output_dict)}")

    return output_dict


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    # Getting service delivery time series
    logging.info("Fetch[ing] data...")
    se_timeseries_df = get_data(SE_DATA_FILENAME,
                                secrets["minio"]["edge"]["access"],
                                secrets["minio"]["edge"]["secret"])
    se_timeseries_df[DATE_COL] = pandas.to_datetime(se_timeseries_df[DATE_COL], format=DATE_FORMAT)
    logging.info("Fetch[ed] data")

    logging.info("Get[ing] latest values data...")
    latest_se_values = create_latest_se_values_dict(se_timeseries_df)
    latest_se_values_json = to_json_data(latest_se_values)
    logging.info("G[ot] latest values data")

    logging.info("Writ[ing] everything to Minio...")
    write_to_minio(latest_se_values_json, OUTPUT_VALUE_FILENAME,
                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")
