import json
import logging
import os
import pprint
import sys
import tempfile

from db_utils import minio_utils
import pandas

import city_map_layers_to_minio

MINIO_COVID_BUCKET = "covid"
MINIO_HEX_BUCKET = "city-hex-polygons"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_PUBLIC_PREFIX = "data/public/"
DATA_RESTRICTED_PREFIX = "data/private/"
CASE_MAP_PREFIX = "widgets/private/case_count_maps/"

PROV_CASE_DATA_FILENAME = "wc_all_cases.csv"
PROV_CASE_FILE_ENCODING = "iso-8859-1"

WARD_COUNT_SUFFIX = "ward_case_count.geojson"
HEX_L7_COUNT_SUFFIX = "hex_l7_case_count.geojson"
HEX_L8_COUNT_SUFFIX = "hex_l8_case_count.geojson"
DISTRICT_COUNT_SUFFIX = "district_case_count.geojson"

CHOROPLETH_LAYERS = (
    WARD_COUNT_SUFFIX,
    HEX_L7_COUNT_SUFFIX,
    HEX_L8_COUNT_SUFFIX,
    DISTRICT_COUNT_SUFFIX
)
CT_HEX_L7_FILENAME = "city-hex-polygons-7.geojson"
CT_HEX_L8_FILENAME = "city-hex-polygons-8.geojson"
CT_WARD_FILENAME = "ct_wards.geojson"
CT_HEALTH_DISTRICT_FILENAME = "health_districts.geojson"
CHOROPLETH_SOURCE_LAYERS = {
    HEX_L7_COUNT_SUFFIX: CT_HEX_L7_FILENAME,
    HEX_L8_COUNT_SUFFIX: CT_HEX_L8_FILENAME,
    WARD_COUNT_SUFFIX: CT_WARD_FILENAME,
    DISTRICT_COUNT_SUFFIX: CT_HEALTH_DISTRICT_FILENAME
}

LAYER_FILES = (
    (CT_HEX_L7_FILENAME, MINIO_HEX_BUCKET, ""),
    (CT_HEX_L8_FILENAME, MINIO_HEX_BUCKET, ""),
    (CT_WARD_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    (CT_HEALTH_DISTRICT_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
)

HEX_COUNT_INDEX_PROPERTY = "index"
CHOROPLETH_COL_LOOKUP = {
    # filename: (col name in gdf, col name in case count df)
    WARD_COUNT_SUFFIX: (
        "WardID", "Ward.Number",
        lambda ward: (str(int(ward)) if pandas.notna(ward) else None)
    ),
    HEX_L7_COUNT_SUFFIX: (HEX_COUNT_INDEX_PROPERTY, "hex_l7", lambda hex: hex),
    HEX_L8_COUNT_SUFFIX: (HEX_COUNT_INDEX_PROPERTY, "hex_l8", lambda hex: hex),
    DISTRICT_COUNT_SUFFIX: ("CITY_HLTH_RGN_NAME", "Subdistrict", lambda district: district.upper()),
}

ACTIVE_WINDOW = pandas.Timedelta(days=14)

DISTRICT_COL = "District"
SUBDISTRICT_COL = "Subdistrict"
DATE_DIAGNOSIS_COL = "Date.of.Diagnosis"
DATE_DEATH_COL = "Date.of.Death"
DATE_ADMITTED_COL = "Admission.Date"
HOSPITALISED_COL = "Hospitalised"
ICU_COL = "Admitted.to.ICU"
DIED_COL = "Died"

ACTIVE_METADATA_KEY = "Active"
CUMULATIVE_METADATA_KEY = "All"
DEATHS_METADATA_KEY = "Deaths"
HOSPITALISED_METADATA_KEY = "Hospitalised"
ICU_METADATA_KEY = "ICU"

ACTIVE_CASE_COUNT_COL = "ActiveCaseCount"
CASE_COUNT_COL = "CaseCount"
DEATHS_COUNT_COL = "DeathsCount"

CASE_COUNT_KEY = "CountCol"
NOT_SPATIAL_CASE_COUNT = "not_spatial_count"
CASE_COUNT_TOTAL = "total_count"
LATEST_INCREASE = "latest_increase"
LATEST_RELATIVE_INCREASE = "latest_relative_increase"

REPORTING_PERIOD = pandas.Timedelta(days=7)
REPORTING_DELAY = pandas.Timedelta(days=3)

DELTA_SUFFIX = "Delta"
RELATIVE_DELTA_SUFFIX = "RelativeDelta"


def get_case_data(minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_datafile:
        minio_utils.minio_to_file(
            filename=temp_datafile.name,
            minio_filename_override=DATA_RESTRICTED_PREFIX + PROV_CASE_DATA_FILENAME,
            minio_bucket=MINIO_COVID_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        case_data_df = pandas.read_csv(temp_datafile.name, encoding=PROV_CASE_FILE_ENCODING)

        for col in (DATE_DIAGNOSIS_COL, DATE_ADMITTED_COL, DATE_DEATH_COL):
            case_data_df[col] = pandas.to_datetime(case_data_df[col])

    return case_data_df


def filter_district_case_data(case_data_df, district_name, subdistrict_name):
    district_filter = case_data_df[DISTRICT_COL].str.lower() == district_name if district_name != "*" else case_data_df[
        DATE_DIAGNOSIS_COL].notna()
    logging.debug(f"district / all cases {district_filter.sum()} / {district_filter.shape[0]}")

    # Horrible hack due to subdistrict column renaming
    case_data_df[SUBDISTRICT_COL] = case_data_df[SUBDISTRICT_COL].apply(
        lambda subdistrict: subdistrict.split(" - ")[-1]
    )

    district_filter &= (
        case_data_df[SUBDISTRICT_COL].str.lower() == subdistrict_name if subdistrict_name != "*" else True
    )
    logging.debug(f"subdistrict / all cases {district_filter.sum()} / {district_filter.shape[0]}")

    district_filter &= ~case_data_df[DATE_DIAGNOSIS_COL].isna()
    logging.debug(f"valid diagnosis dates / all cases {district_filter.sum()} / {district_filter.shape[0]}")

    return case_data_df[district_filter]


def filter_active_case_data(case_data_df):
    latest_date = case_data_df[DATE_DIAGNOSIS_COL].max()
    logging.debug(f"Latest date seen: {latest_date.strftime('%Y-%m-%d')}")

    active_window = latest_date - ACTIVE_WINDOW
    logging.debug(f"Assuming all cases since {active_window.strftime('%Y-%m-%d')} are active")

    active_filter = case_data_df[DATE_DIAGNOSIS_COL] >= active_window
    active_filter &= ((case_data_df[DIED_COL] != "Yes") | (case_data_df[DATE_DEATH_COL] > latest_date))

    logging.debug(f"Active / Total cases {active_filter.sum()} / {active_filter.shape[0]}")

    return case_data_df[active_filter]


def filter_deaths_case_data(case_data_df):
    death_filter = case_data_df[DIED_COL] == "Yes"

    logging.debug(f"Deaths / Total cases {death_filter.sum()} / {death_filter.shape[0]}")

    return case_data_df[death_filter]


def filter_hospitalised_case_data(case_data_df):
    hospital_filter = case_data_df[HOSPITALISED_COL] == "Yes"

    logging.debug(f"Hospitalised / Total cases {hospital_filter.sum()} / {hospital_filter.shape[0]}")

    return case_data_df[hospital_filter]


def filter_icu_case_data(case_data_df):
    icu_filter = case_data_df[ICU_COL] == "Yes"

    logging.debug(f"Admitted to ICU / Total cases {icu_filter.sum()} / {icu_filter.shape[0]}")

    return case_data_df[icu_filter]


CASE_COL_FILTER_FUNC_MAP = ((
    (CASE_COUNT_COL, (lambda df: df, CUMULATIVE_METADATA_KEY)),
    (ACTIVE_CASE_COUNT_COL, (filter_active_case_data, ACTIVE_METADATA_KEY)),
    (DEATHS_COUNT_COL, (filter_deaths_case_data, DEATHS_METADATA_KEY)),
    (HOSPITALISED_COL, (filter_hospitalised_case_data, HOSPITALISED_METADATA_KEY)),
    (ICU_COL, (filter_icu_case_data, ICU_METADATA_KEY))
))


def spatialise_case_data(case_data_df, case_data_groupby_index, data_gdf, data_gdf_index, fill_nas=False):
    case_count_gdf = data_gdf.copy().set_index(data_gdf_index)

    # Doing the time period Maths
    most_recent = case_data_df[DATE_DIAGNOSIS_COL].max() - REPORTING_DELAY
    previous_period_end = most_recent - REPORTING_PERIOD
    logging.debug(f"previous_period_end={previous_period_end}")
    previous_cases_data_df = case_data_df.query(f"`{DATE_DIAGNOSIS_COL}` <= @previous_period_end")

    for col, (filter_case_data_func, _) in CASE_COL_FILTER_FUNC_MAP:
        # Straight Count
        counts = filter_case_data_func(case_data_df).assign(**{col: 1}).groupby(
            case_data_groupby_index
        ).sum()[col]
        case_count_gdf[col] = counts

        # Delta Calculation
        previous_counts = filter_case_data_func(previous_cases_data_df).assign(**{col: 1}).groupby(
            case_data_groupby_index
        ).sum()[col]

        case_count_gdf[col + DELTA_SUFFIX] = (counts - previous_counts)
        # Calculating Relative Delta
        case_count_gdf[col + RELATIVE_DELTA_SUFFIX] = case_count_gdf[col + DELTA_SUFFIX]/case_count_gdf[col]

        # Handling NaN deltas
        na_mask = case_count_gdf[col + DELTA_SUFFIX].isna()
        # where there have been new cases
        new_counts = counts.index.isin(case_count_gdf[na_mask].index)
        new_counts_mask = na_mask & case_count_gdf.index.isin(counts.index)
        case_count_gdf.loc[new_counts_mask, col + DELTA_SUFFIX] = counts.loc[new_counts]
        # where there were cases, but there are no more
        old_counts = previous_counts.index.isin(case_count_gdf[na_mask].index)
        old_counts_mask = na_mask & case_count_gdf.index.isin(previous_counts.index)
        case_count_gdf.loc[old_counts_mask, col + DELTA_SUFFIX] = -(previous_counts.loc[old_counts])

        if fill_nas:
            case_count_gdf[col].fillna(0, inplace=True)
            case_count_gdf[col + DELTA_SUFFIX].fillna(0, inplace=True)
            case_count_gdf[col + RELATIVE_DELTA_SUFFIX].fillna(0, inplace=True)

        logging.debug(
            f"case_count_gdf.sort_values(by='{col}', ascending=False).head(5)=\n"
            f"{case_count_gdf.sort_values(by=col, ascending=False).head(5)}"
        )
        logging.debug(
            f"case_count_gdf.sort_values(by='{col + DELTA_SUFFIX}', ascending=False).head(5)=\n"
            f"{case_count_gdf.sort_values(by=col + DELTA_SUFFIX, ascending=False).head(5)}"
        )

    return case_count_gdf


def compress_gdf(case_count_gdf):
    # Removing what we can
    case_count_gdf.dropna(
        subset=(
            [col for col, _ in CASE_COL_FILTER_FUNC_MAP] + [col + DELTA_SUFFIX for col, _ in CASE_COL_FILTER_FUNC_MAP]
        ), how="all", inplace=True
    )

    return case_count_gdf


def calculate_latest_increase(case_data_df, relative=False):
    daily_counts = case_data_df.groupby(DATE_DIAGNOSIS_COL).count()[DIED_COL].rename("DailyCases")
    logging.debug(f"daily_counts=\n{daily_counts}")

    # Doing the time period Maths
    most_recent = daily_counts.index.max() - REPORTING_DELAY
    previous_period_end = most_recent - REPORTING_PERIOD
    previous_period_start = previous_period_end - REPORTING_PERIOD

    # Getting the median values. If NaN, it means there weren't any cases in that period
    most_recent_period_median = daily_counts[previous_period_end:most_recent].median()
    most_recent_period_median = most_recent_period_median if pandas.notna(most_recent_period_median) else 0

    previous_period_median = daily_counts[previous_period_start:previous_period_end].median()
    previous_period_median = previous_period_median if pandas.notna(previous_period_median) else 0

    delta = most_recent_period_median - previous_period_median
    delta /= previous_period_median if relative else 1

    logging.debug(f"most_recent={most_recent}, previous_period_end={previous_period_end}, delta={delta}")

    return delta


def generate_metadata(case_data_df, case_data_groupby_index):
    metadata_dict = {
        metadata_key: {
            CASE_COUNT_KEY: case_count_col,
            NOT_SPATIAL_CASE_COUNT: int(
                filter_case_data_func(case_data_df)[case_data_groupby_index].isna().sum()
            ),
            CASE_COUNT_TOTAL: int(filter_case_data_func(case_data_df).shape[0]),
            LATEST_INCREASE: int(calculate_latest_increase(filter_case_data_func(case_data_df))),
            LATEST_RELATIVE_INCREASE: float(
                calculate_latest_increase(filter_case_data_func(case_data_df), relative=True))
        }
        for case_count_col, (filter_case_data_func, metadata_key) in CASE_COL_FILTER_FUNC_MAP
    }
    logging.debug(f"metadata_dict={metadata_dict}")

    return metadata_dict


def write_case_count_gdf_to_disk(case_count_data_gdf, tempdir, case_count_filename):
    local_path = os.path.join(tempdir, case_count_filename)
    case_count_data_gdf.reset_index().to_file(local_path, driver='GeoJSON')

    return local_path, case_count_data_gdf


def write_metadata_to_minio(metadata_dict, tempdir, metadata_filename, minio_access, minio_secret):
    local_path = os.path.join(tempdir, metadata_filename)
    with open(local_path, "w") as not_spatial_case_count_file:
        json.dump(metadata_dict, not_spatial_case_count_file)

    result = minio_utils.file_to_minio(
        filename=local_path,
        filename_prefix_override=CASE_MAP_PREFIX,
        minio_bucket=MINIO_COVID_BUCKET,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=MINIO_CLASSIFICATION,
    )

    assert result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    district_file_prefix = sys.argv[1]
    district_name = sys.argv[2]

    subdistrict_file_prefix = sys.argv[3]
    subdistrict_name = sys.argv[4]
    logging.info(f"Generat[ing] map layers for '{district_name}' district, '{subdistrict_name}' subdistrict")

    # Has to be in the outer scope as the tempdir is used in multiple places
    with tempfile.TemporaryDirectory() as tempdir:
        logging.info("G[etting] layers")
        map_layers_dict = {
            layer: (local_path, layer_gdf)
            for layer, local_path, layer_gdf in city_map_layers_to_minio.get_layers(tempdir,
                                                                                    secrets["minio"]["edge"]["access"],
                                                                                    secrets["minio"]["edge"]["secret"],
                                                                                    layers=LAYER_FILES)
        }
        logging.info("G[ot] layers")

        logging.info("G[etting] Case Data")
        cases_df = get_case_data(secrets["minio"]["edge"]["access"],
                                 secrets["minio"]["edge"]["secret"])
        logging.info("G[ot] Case Data")

        logging.info("Filter[ing] to District Level")
        filtered_df = filter_district_case_data(cases_df, district_name, subdistrict_name)
        logging.info("Filter[ed] to District Level")

        # Generating choropleths based upon case count
        for layer_suffix in CHOROPLETH_LAYERS:
            layer_filename = f"{district_file_prefix}_{subdistrict_file_prefix}_{layer_suffix}"

            gdf_property, df_col, sanitise_func = CHOROPLETH_COL_LOOKUP[layer_suffix]
            logging.debug(f"gdf_property={gdf_property}, df_col={df_col}")

            logging.info(f"Count[ing] cases for '{layer_filename}'")
            source_layer = CHOROPLETH_SOURCE_LAYERS[layer_suffix]
            _, data_gdf = map_layers_dict[source_layer]
            logging.debug(f"cases_df.columns=\n{filtered_df.columns}")
            filtered_df[df_col] = filtered_df[df_col].apply(sanitise_func)

            case_count_gdf = spatialise_case_data(filtered_df, df_col,
                                                  data_gdf, gdf_property)
            case_count_gdf = compress_gdf(case_count_gdf)
            layer_metadata = generate_metadata(filtered_df, df_col)
            logging.info(f"Count[ed] cases for '{layer_filename}'")

            logging.info(f"Writ[ing] geojson for '{layer_filename}'")
            count_layer_values = write_case_count_gdf_to_disk(case_count_gdf, tempdir, layer_filename)
            map_layers_dict[layer_filename] = count_layer_values
            logging.info(f"Wr[ote] geojson for '{layer_filename}'")

            logging.info(f"Writ[ing] metadata for '{layer_filename}'")
            logging.debug(f"layer_metadata=\n{pprint.pformat(layer_metadata)}")
            layer_stem, layer_ext = os.path.splitext(layer_filename)
            metadata_filename = layer_stem + ".json"
            write_metadata_to_minio(layer_metadata, tempdir, metadata_filename,
                                    secrets["minio"]["edge"]["access"],
                                    secrets["minio"]["edge"]["secret"])
            logging.info(f"Wr[ote] metadata for '{layer_filename}'")

        logging.info("Writ[ing] layers to Minio")
        city_map_layers_to_minio.write_layers_to_minio(map_layers_dict,
                                                       secrets["minio"]["edge"]["access"],
                                                       secrets["minio"]["edge"]["secret"])
        logging.info("Wr[ote] layers to Minio")
