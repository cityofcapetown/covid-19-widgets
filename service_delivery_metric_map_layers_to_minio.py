import json
import logging
import os
import pprint
import sys
import tempfile

from db_utils import minio_utils
import pandas

import city_map_layers_to_minio
import epi_map_case_layers_to_minio
import service_delivery_latest_values_to_minio
import service_request_map_layers_to_minio

MINIO_COVID_BUCKET = "covid"
MINIO_HEX_BUCKET = "city-hexes.polygons"
MINIO_EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
MINIO_LAKE_CLASSIFICATION = minio_utils.DataClassification.LAKE

SD_METRIC_DATA_FILENAME = "business_continuity_service_delivery_spatial_metrics.csv"
FEATURE_COL = "feature"
TOTAL_COLS = [
    "service_standard_weighting_delta", "long_backlog_weighting_delta", "backlog"
]
TOTAL_SUFFIX = "_total"

DATA_PUBLIC_PREFIX = "data/public/"
DATA_RESTRICTED_PREFIX = "data/private/"
SERVICE_DELIVERY_MAP_PREFIX = "widgets/private/business_continuity_service_delivery_map/"

HEX_METRIC_L7_FILENAME = "hex_l7_service_delivery_metrics.geojson"
CHOROPLETH_LAYERS = (
    HEX_METRIC_L7_FILENAME,
)
CT_HEX_L7_FILENAME = "city-hex-polygons-7.geojson"

HEX_INDEX_COL = "index"
THRESHOLD_COL = "long_backlog_weighting_delta"

PERCENT_COLS = ["service_standard", "service_standard_delta", "long_backlog", "long_backlog_delta",
                "backlog_delta_relative"]

THRESHOLD_MIN_VALUES = 100
THRESHOLD_VALUES = 0.005

CHOROPLETH_SOURCE_ATTRS = {
    # filename: source_filename, index_col, NULL Value
    HEX_METRIC_L7_FILENAME: (CT_HEX_L7_FILENAME, HEX_INDEX_COL, '0'),
}

LAYER_FILES = (
    (CT_HEX_L7_FILENAME, MINIO_LAKE_CLASSIFICATION, MINIO_HEX_BUCKET, ""),
)


def format_percent_value(key, value):
    val = value
    if key in PERCENT_COLS:
        val = round(val * 100, 1)

    return val


def generate_indexed_dict(metrics_df, index_col, index_null_value):
    initial_dict = metrics_df.set_index([index_col, FEATURE_COL]).to_dict(orient='index')
    logging.debug(f"len(initial_dict)={len(initial_dict)}")

    percentile_mins_df = (metrics_df.groupby(FEATURE_COL)[THRESHOLD_COL].sum()*THRESHOLD_VALUES).to_frame()
    percentile_mins_df["dummy"] = THRESHOLD_MIN_VALUES

    logging.debug(f"percentile_mins={percentile_mins_df}")
    threshold_limits = percentile_mins_df.min(axis='columns')
    logging.debug(f"Using minimum value of {threshold_limits} per index")

    nested_dict = {}
    for (index, feature), values_dict in initial_dict.items():
        index_dict = nested_dict.get(index, {})
        null_dict = nested_dict.get(index_null_value, {})

        with pandas.option_context('mode.use_inf_as_na', True):
            sanitised_values = {
                f"{feature}-{k}": format_percent_value(k, v) for k, v in values_dict.items() if pandas.notna(v)
            }

        threshold_val = sanitised_values.get(f"{feature}-{THRESHOLD_COL}", 0)

        if len(sanitised_values) and threshold_val >= threshold_limits[feature]:
            index_dict = {**sanitised_values, **index_dict}
            nested_dict[index] = index_dict
        elif threshold_val < threshold_limits[feature] or index == index_null_value:
            logging.warning(f"below threshold - skipping '{feature}' for '{index}' ({index_col})!")
            null_dict[f"{feature}-{THRESHOLD_COL}"] = null_dict.get(f"{feature}-{THRESHOLD_COL}", 0) + threshold_val
            nested_dict[index_null_value] = null_dict
        else:
            logging.warning(f"insufficent non-null values - skipping '{feature}' for '{index}' ('{index_col}')!")

    feature_df = pandas.DataFrame.from_dict(nested_dict, orient='index')

    return feature_df


def generate_spatial_gdf(spatial_layer_gdf, index_col, feature_df):
    spatial_index_gdf = spatial_layer_gdf.set_index(index_col)

    merged_gdf = spatial_index_gdf.merge(
        feature_df,
        left_index=True, right_index=True,
        validate="one_to_one",
        how='left'
    )

    return merged_gdf


def generate_metrics_metadata(data_df, index_col, index_null_value):
    non_spatial_df = data_df[
        data_df[index_col] == index_null_value
        ].drop(
        columns=[index_col]
    ).set_index(
        FEATURE_COL
    )

    total_df = data_df.groupby(FEATURE_COL)[TOTAL_COLS].sum().add_suffix(
        TOTAL_SUFFIX
    )

    non_spatial_df = non_spatial_df.combine_first(
        total_df
    )

    with pandas.option_context('mode.use_inf_as_na', True):
        metadata_dict = {
            k: {
                sk:sv for sk, sv in v.items()
                if pandas.notna(sv)
            }
            for k, v in non_spatial_df.to_dict(orient='index').items()
            if pandas.notna(v)
        }

    # Ensuring the expected spatial/total count values are present in metadata for each feature
    for feature_dict in metadata_dict.values():
        feature_dict[epi_map_case_layers_to_minio.NOT_SPATIAL_CASE_COUNT] = feature_dict.get(THRESHOLD_COL, 0)
        feature_dict[epi_map_case_layers_to_minio.CASE_COUNT_TOTAL] = feature_dict.get(THRESHOLD_COL + TOTAL_SUFFIX, 0)

    return metadata_dict


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
            for layer, local_path, layer_gdf in city_map_layers_to_minio.get_layers(tempdir,
                                                                                    secrets["minio"]["edge"]["access"],
                                                                                    secrets["minio"]["edge"]["secret"],
                                                                                    LAYER_FILES,
                                                                                    secrets["minio"]["lake"]["access"],
                                                                                    secrets["minio"]["lake"][
                                                                                        "secret"], )
        }
        logging.info("G[ot] layers")

        logging.info("G[etting] SD Data")
        sd_metric_data_df = service_delivery_latest_values_to_minio.get_data(
            SD_METRIC_DATA_FILENAME,
            secrets["minio"]["edge"]["access"],
            secrets["minio"]["edge"]["secret"]
        )
        logging.info("G[ot] SD Data")

        for layer_filename in CHOROPLETH_LAYERS:
            source_layer, source_layer_index_col, null_value = CHOROPLETH_SOURCE_ATTRS[layer_filename]
            _, data_gdf = map_layers_dict[source_layer]

            logging.info(f"Generat[ing] metrics indexed for '{layer_filename}'")
            indexed_df = generate_indexed_dict(sd_metric_data_df, source_layer_index_col, null_value)
            logging.info(f"Generat[ed] metrics indexed for '{layer_filename}'")

            logging.info(f"Merg[ing] metrics indexed for '{layer_filename}'")
            indexed_gdf = generate_spatial_gdf(data_gdf, source_layer_index_col, indexed_df)
            logging.info(f"Merg[ed] metrics indexed for '{layer_filename}'")

            logging.info(f"Writ[ing] geojson for '{layer_filename}'")
            metric_layer_values = service_request_map_layers_to_minio.write_service_request_count_gdf_to_disk(
                indexed_gdf, tempdir, layer_filename
            )
            map_layers_dict[layer_filename] = metric_layer_values
            logging.info(f"Wr[ote] geojson for '{layer_filename}'")

            logging.info(f"Generat[ing] metadata for '{layer_filename}'")
            layer_metadata = generate_metrics_metadata(sd_metric_data_df, source_layer_index_col, null_value)
            logging.debug(f"layer_metadata=\n{pprint.pformat(layer_metadata)}")
            logging.info(f"Generat[ed] metadata for '{layer_filename}'")

            logging.info(f"Writ[ing] metadata for '{layer_filename}'")
            layer_stem, layer_ext = os.path.splitext(layer_filename)
            metadata_filename = layer_stem + ".json"
            service_request_map_layers_to_minio.write_metadata_to_minio(layer_metadata, tempdir, metadata_filename,
                                                                        secrets["minio"]["edge"]["access"],
                                                                        secrets["minio"]["edge"]["secret"],
                                                                        map_prefix=SERVICE_DELIVERY_MAP_PREFIX)
            logging.info(f"Wr[ote] metadata for '{layer_filename}'")

        logging.info("Writ[ing] layers to Minio")
        city_map_layers_to_minio.write_layers_to_minio(map_layers_dict,
                                                       secrets["minio"]["edge"]["access"],
                                                       secrets["minio"]["edge"]["secret"],
                                                       prefix=SERVICE_DELIVERY_MAP_PREFIX)
        logging.info("Wr[ote] layers to Minio")
