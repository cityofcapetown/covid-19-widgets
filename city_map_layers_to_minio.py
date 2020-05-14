import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import geopandas
import pandas

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_PUBLIC_PREFIX = "data/public/"
DATA_RESTRICTED_PREFIX = "data/private/"
WIDGETS_RESTRICTED_PREFIX = "widgets/private/city_map_"

CITY_CASE_DATA_FILENAME = "ct_all_cases.csv"

WARD_COUNT_FILENAME = "ward_case_count.geojson"
HEX_COUNT_FILENAME = "hex_case_count.geojson"
CHOROPLETH_LAYERS = (
    WARD_COUNT_FILENAME,
    HEX_COUNT_FILENAME,
)
CHOROPLETH_SOURCE_LAYERS = {
    HEX_COUNT_FILENAME: "cct_hex_polygons_7.geojson",
    WARD_COUNT_FILENAME: "ct_wards.geojson"
}

LAYER_FILES = (
    "informal_settlements.geojson",
    "health_care_facilities.geojson",
    *CHOROPLETH_SOURCE_LAYERS.values(),
    *CHOROPLETH_LAYERS
)

HEX_COUNT_INDEX_PROPERTY = "index"
CHOROPLETH_COL_LOOKUP = {
    # filename: (col name in gdf, col name in case count df)
    WARD_COUNT_FILENAME: (
        "WardID", "Ward.Number",
        lambda ward: (str(int(ward)) if pandas.notna(ward) else None)
    ),
    HEX_COUNT_FILENAME: (HEX_COUNT_INDEX_PROPERTY, "hex_l7", lambda hex: hex)
}

CASE_COUNT_COL_OLD = "Date.of.Diagnosis"
CASE_COUNT_COL = "CaseCount"


def get_layers(tempdir, minio_access, minio_secret):
    for layer in LAYER_FILES:
        if layer not in CHOROPLETH_LAYERS:
            local_path = os.path.join(tempdir, layer)

            minio_utils.minio_to_file(
                filename=local_path,
                minio_filename_override=DATA_PUBLIC_PREFIX + layer,
                minio_bucket=MINIO_BUCKET,
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
            minio_filename_override=DATA_RESTRICTED_PREFIX + CITY_CASE_DATA_FILENAME,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        case_data_df = pandas.read_csv(temp_datafile.name)

    return case_data_df


def spatialise_case_data(case_data_df, case_data_groupby_index, data_gdf, data_gdf_index):
    case_counts = case_data_df.groupby(
        case_data_groupby_index
    ).count()[CASE_COUNT_COL_OLD].rename(CASE_COUNT_COL)

    case_count_gdf = data_gdf.copy().set_index(data_gdf_index)
    case_count_gdf[CASE_COUNT_COL] = case_counts
    case_count_gdf[CASE_COUNT_COL].fillna(0, inplace=True)
    logging.debug(f"case_count_gdf.head(5)=\n{case_count_gdf.head(5)}")

    return case_count_gdf


def write_case_count_gdf_to_disk(case_count_data_gdf, tempdir, case_count_filename):
    local_path = os.path.join(tempdir, case_count_filename)
    case_count_data_gdf.reset_index().to_file(local_path, driver='GeoJSON')

    return local_path, case_count_data_gdf


def write_layers_to_minio(layers_dict, minio_access, minio_secret):
    for layer_name, (layer_local_path, _) in layers_dict.items():
        result = minio_utils.file_to_minio(
            filename=layer_local_path,
            filename_prefix_override=WIDGETS_RESTRICTED_PREFIX,
            minio_bucket=MINIO_BUCKET,
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

    # Has to be in the outer scope as use the tempdir in multiple places
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

        # Generating choropleths based upon case count
        for layer_filename in CHOROPLETH_LAYERS:
            gdf_property, df_col, sanitise_func = CHOROPLETH_COL_LOOKUP[layer_filename]
            logging.debug(f"gdf_property={gdf_property}, df_col={df_col}")

            logging.info(f"Count[ing] cases for '{layer_filename}'")
            source_layer = CHOROPLETH_SOURCE_LAYERS[layer_filename]
            _, data_gdf = map_layers_dict[source_layer]
            logging.debug(f"cases_df.columns=\n{cases_df.columns}")
            cases_df[df_col] = cases_df[df_col].apply(sanitise_func)
            case_count_gdf = spatialise_case_data(cases_df, df_col,
                                                  data_gdf, gdf_property)
            logging.info(f"Count[ed] cases for '{layer_filename}'")

            logging.info(f"Writ[ing] geojson for '{layer_filename}'")
            count_layer_values = write_case_count_gdf_to_disk(case_count_gdf, tempdir, layer_filename)
            map_layers_dict[layer_filename] = count_layer_values
            logging.info(f"Wr[ote] geojson for '{layer_filename}'")

        logging.info("Writ[ing] layers to Minio")
        write_layers_to_minio(map_layers_dict,
                              secrets["minio"]["edge"]["access"],
                              secrets["minio"]["edge"]["secret"])
        logging.info("Wr[ote] layers to Minio")