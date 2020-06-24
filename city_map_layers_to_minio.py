import json
import logging
import os
import pprint
import sys
import tempfile

from db_utils import minio_utils
import geopandas
import pandas

MINIO_COVID_BUCKET = "covid"
MINIO_HEX_BUCKET = "city-hex-polygons"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_PUBLIC_PREFIX = "data/public/"
DATA_VULNERABILITY_PREFIX = "data/staging/vulenerability_layers/"  # yes, I know it's misspelled - should be temporary
DATA_RESTRICTED_PREFIX = "data/private/"
CASE_MAP_PREFIX = "widgets/staging/case_count_maps/"

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
    ("informal_settlements.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("health_care_facilities.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    (CT_HEX_L7_FILENAME, MINIO_HEX_BUCKET, ""),
    (CT_HEX_L8_FILENAME, MINIO_HEX_BUCKET, ""),
    (CT_WARD_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    (CT_HEALTH_DISTRICT_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("wcpg_testing_facilities.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("sl_du_pop_est_2019_hex9.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("province_sevi_hex9.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("wced_metro_schools_2019.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("retail_stores.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("shopping_centres_above_5000sqm_rode_2020.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("public_transport_interchanges.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("public_transport_activity_levels_hex9.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("trading_location.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("sassa_local_office_coc.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("city_house_counts.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("city_flats_counts.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("city_hostel_counts.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("areas_of_informality_2019.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("hdx_pop_estimates_elderly_hex9.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("adult_homeless_shelters_coct.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("employment_density_survey_hex7.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("hh_emp_incomegrp_sp_tz2018_hex8.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("combined_senior_citizens_layer.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX)
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
DIED_COL = "Died"

ACTIVE_METADATA_KEY = "Active"
CUMULATIVE_METADATA_KEY = "All"
DEATHS_METADATA_KEY = "Deaths"

ACTIVE_CASE_COUNT_COL = "ActiveCaseCount"
CASE_COUNT_COL = "CaseCount"
DEATHS_COUNT_COL = "DeathsCount"

CASE_COUNT_KEY = "CountCol"
NOT_SPATIAL_CASE_COUNT = "not_spatial_count"
CASE_COUNT_TOTAL = "total_count"
LATEST_INCREASE = "latest_increase"

REPORTING_PERIOD = pandas.Timedelta(days=3)
REPORTING_DELAY = pandas.Timedelta(days=3)


def get_layers(tempdir, minio_access, minio_secret):
    for layer, layer_bucket, layer_minio_prefix in LAYER_FILES:
        local_path = os.path.join(tempdir, layer)

        minio_utils.minio_to_file(
            filename=local_path,
            minio_filename_override=layer_minio_prefix + layer,
            minio_bucket=layer_bucket,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        layer_gdf = geopandas.read_file(local_path)

        yield layer, local_path, layer_gdf


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
        case_data_df[DATE_DIAGNOSIS_COL] = pandas.to_datetime(case_data_df[DATE_DIAGNOSIS_COL])

    return case_data_df


def filter_district_case_data(case_data_df, district_name, subdistrict_name):
    district_filter = case_data_df[DISTRICT_COL].str.lower() == district_name if district_name != "*" else case_data_df[
        DATE_DIAGNOSIS_COL].notna()
    logging.debug(f"district / all cases {district_filter.sum()} / {district_filter.shape[0]}")

    district_filter &= case_data_df[
                           SUBDISTRICT_COL].str.lower() == subdistrict_name if subdistrict_name != "*" else True
    logging.debug(f"subdistrict / all cases {district_filter.sum()} / {district_filter.shape[0]}")

    return case_data_df[district_filter]


def filter_active_case_data(case_data_df):
    latest_date = case_data_df[DATE_DIAGNOSIS_COL].max()
    logging.debug(f"Latest date seen: {latest_date.strftime('%Y-%m-%d')}")

    active_window = latest_date - ACTIVE_WINDOW
    logging.debug(f"Assuming all cases since {active_window.strftime('%Y-%m-%d')} are active")

    active_filter = case_data_df[DATE_DIAGNOSIS_COL] >= active_window
    active_filter &= case_data_df[DIED_COL] != "Yes"

    logging.debug(f"Active / Total cases {active_filter.sum()} / {active_filter.shape[0]}")

    return case_data_df[active_filter]


def filter_deaths_case_data(case_data_df):
    death_filter = case_data_df[DIED_COL] == "Yes"

    logging.debug(f"Deaths / Total cases {death_filter.sum()} / {death_filter.shape[0]}")

    return case_data_df[death_filter]


def spatialise_case_data(case_data_df, case_data_groupby_index, data_gdf, data_gdf_index, fill_nas=False):
    case_counts = case_data_df.groupby(
        case_data_groupby_index
    ).count()[DATE_DIAGNOSIS_COL].rename(CASE_COUNT_COL)

    active_case_counts = filter_active_case_data(case_data_df).groupby(
        case_data_groupby_index
    ).count()[DATE_DIAGNOSIS_COL].rename(ACTIVE_CASE_COUNT_COL)

    fatal_case_counts = filter_deaths_case_data(case_data_df).groupby(
        case_data_groupby_index
    ).count()[DATE_DEATH_COL].rename(DEATHS_COUNT_COL)

    case_count_gdf = data_gdf.copy().set_index(data_gdf_index)

    for col, counts in ((CASE_COUNT_COL, case_counts),
                        (ACTIVE_CASE_COUNT_COL, active_case_counts),
                        (DEATHS_COUNT_COL, fatal_case_counts)):
        case_count_gdf[col] = counts
        if fill_nas:
            case_count_gdf[col].fillna(0, inplace=True)
        else:
            case_count_gdf.dropna(subset=[col], inplace=True)

        logging.debug(
            f"case_count_gdf.sort_values(by='{col}', ascending=False).head(5)=\n"
            f"{case_count_gdf.sort_values(by=col, ascending=False).head(5)}"
        )

    return case_count_gdf


def calculate_latest_increase(case_data_df):
    daily_counts = case_data_df.groupby(DATE_DIAGNOSIS_COL).count()[DIED_COL].rename("DailyCases")
    logging.debug(f"daily_counts=\n{daily_counts}")

    most_recent = daily_counts.index.max() - REPORTING_DELAY
    previous_period_end = most_recent - REPORTING_PERIOD
    previous_period_start = previous_period_end - REPORTING_PERIOD

    delta = daily_counts[previous_period_end:most_recent].median() - daily_counts[
                                                                     previous_period_start:previous_period_end].median()
    logging.debug(f"most_recent={most_recent}, previous_period_end={previous_period_end}, delta={delta}")

    return delta


def generate_metadata(case_data_df, case_data_groupby_index):
    metadata_dict = {
        metadata_key: {
            CASE_COUNT_KEY: case_count_col,
            NOT_SPATIAL_CASE_COUNT: int(metadata_df[case_data_groupby_index].isna().sum()),
            CASE_COUNT_TOTAL: int(metadata_df.shape[0]),
            LATEST_INCREASE: int(calculate_latest_increase(metadata_df))
        }
        for metadata_key, case_count_col, metadata_df in (
            (CUMULATIVE_METADATA_KEY, CASE_COUNT_COL, case_data_df),
            (ACTIVE_METADATA_KEY, ACTIVE_CASE_COUNT_COL, filter_active_case_data(case_data_df)),
            (DEATHS_METADATA_KEY, DEATHS_COUNT_COL, filter_deaths_case_data(case_data_df)),
        )
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


def write_layers_to_minio(layers_dict, minio_access, minio_secret):
    for layer_name, (layer_local_path, _) in layers_dict.items():
        result = minio_utils.file_to_minio(
            filename=layer_local_path,
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
            for layer, local_path, layer_gdf in get_layers(tempdir,
                                                           secrets["minio"]["edge"]["access"],
                                                           secrets["minio"]["edge"]["secret"])
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
            # ToDo add a lookup based on the district name
            source_layer = CHOROPLETH_SOURCE_LAYERS[layer_suffix]
            _, data_gdf = map_layers_dict[source_layer]
            logging.debug(f"cases_df.columns=\n{filtered_df.columns}")
            filtered_df[df_col] = filtered_df[df_col].apply(sanitise_func)

            case_count_gdf = spatialise_case_data(filtered_df, df_col,
                                                  data_gdf, gdf_property)
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
        write_layers_to_minio(map_layers_dict,
                              secrets["minio"]["edge"]["access"],
                              secrets["minio"]["edge"]["secret"])
        logging.info("Wr[ote] layers to Minio")
