import json
import pprint
import logging
import os
import sys

import pandas

from hr_data_last_values_to_minio import get_data, to_json_data, write_to_minio

SD_DATA_FILENAME = "business_continuity_service_delivery.csv"

SKIP_LIST = ("safety_and_security", "treasury", "insurance", "library_and_information_services")
REMAP_DICT = {"water_and_waste_services": "water_and_waste"}

DATE_COL = "date"
MEASURE_COL = "measure"
FEATURE_COL = "feature"
VALUE_COL = "value"

METRICS_FIELD = "metrics"
METRICS_DELTA_FIELD = "metrics-delta"
METRICS_DELTA_RELATIVE_FIELD = "metrics-delta-relative"

REFERENCE_DATE = "2020-10-12"

OUTPUT_VALUE_FILENAME = "service_delivery_values.json"


def create_latest_sd_values_dict(ts_df):
    latest_date = ts_df[DATE_COL].max()
    logging.debug(f"latest_date={latest_date}")
    # latest_values_dict = ts_df.query(f"{DATE_COL} == @latest_date").to_dict(orient="records")

    latest_values_dict = ts_df.sort_values(by=[FEATURE_COL, MEASURE_COL, DATE_COL]).drop_duplicates(
        subset=[FEATURE_COL, MEASURE_COL], keep="last"
    ).reset_index().to_dict(orient="records")
    logging.debug(f"latest_values_dict=\n{pprint.pformat(latest_values_dict)}")

    ref_values_dicts = ts_df[
        ts_df[DATE_COL] <= REFERENCE_DATE
    ].sort_values(by=[FEATURE_COL, MEASURE_COL, DATE_COL]).drop_duplicates(
        subset=[FEATURE_COL, MEASURE_COL], keep="last"
    ).reset_index().to_dict(orient="records")

    ref_values_dicts = {
        (ref_values_dict[FEATURE_COL], ref_values_dict[MEASURE_COL]): ref_values_dict
        for ref_values_dict in ref_values_dicts
    }
    logging.debug(f"ref_values_dicts=\n{pprint.pformat(ref_values_dicts)}")

    # transforming from flat records into hierarchy of values
    output_dict = {}
    for value_dict in latest_values_dict:
        feature_values = value_dict[FEATURE_COL].split("-")

        if any([feature_val in SKIP_LIST for feature_val in feature_values]):
            logging.warning(f"Skipping {value_dict[FEATURE_COL]}!")
            continue

        # Creating the relevant location in the output hierachy using the feature
        value_output_dict = output_dict
        for feature in feature_values:
            feature = REMAP_DICT.get(feature, feature)
            if feature not in value_output_dict:
                value_output_dict[feature] = {METRICS_FIELD: {DATE_COL: value_dict[DATE_COL].strftime("%Y-%m-%d")},
                                              METRICS_DELTA_FIELD: {DATE_COL: REFERENCE_DATE},
                                              METRICS_DELTA_RELATIVE_FIELD: {DATE_COL: REFERENCE_DATE}}
                logging.debug(f"Adding {feature} to values dict")

            value_output_dict = value_output_dict[feature]

        value_output_dict[METRICS_FIELD][value_dict[MEASURE_COL]] = value_dict[VALUE_COL]

        ref_lookup_key = (value_dict[FEATURE_COL], value_dict[MEASURE_COL])
        if ref_lookup_key not in ref_values_dicts:
            logging.warning(f"No reference values for '{value_dict[FEATURE_COL]}' for measure '{value_dict[MEASURE_COL]}'")
        else:
            ref_value_dict = ref_values_dicts[ref_lookup_key]
            value_output_dict[METRICS_DELTA_FIELD][value_dict[MEASURE_COL]] = (
                    value_dict[VALUE_COL] - ref_value_dict[VALUE_COL]
            )

            value_output_dict[METRICS_DELTA_RELATIVE_FIELD][value_dict[MEASURE_COL]] = round(
                (value_dict[VALUE_COL] - ref_value_dict[VALUE_COL]) / (
                    ref_value_dict[VALUE_COL] if ref_value_dict[VALUE_COL] else 1e-7
                ), 2
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
    sd_timeseries_df = get_data(SD_DATA_FILENAME,
                                secrets["minio"]["edge"]["access"],
                                secrets["minio"]["edge"]["secret"])
    sd_timeseries_df[DATE_COL] = pandas.to_datetime(sd_timeseries_df[DATE_COL])
    logging.info("Fetch[ed] data")

    logging.info("Get[ing] latest values data...")
    latest_sd_values = create_latest_sd_values_dict(sd_timeseries_df)
    latest_sd_values_json = to_json_data(latest_sd_values)
    logging.info("G[ot] latest values data")

    logging.info("Writ[ing] everything to Minio...")
    write_to_minio(latest_sd_values_json, OUTPUT_VALUE_FILENAME,
                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")
