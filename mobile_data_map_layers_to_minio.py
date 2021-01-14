import json
import logging
import os
import pprint
import sys
import tempfile

from db_utils import minio_utils
import geopandas
import numpy
import pandas

import city_map_layers_to_minio

MINIO_COVID_BUCKET = "covid"
MINIO_HEX_BUCKET = "city-hexes.polygons"
MINIO_EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
MINIO_LAKE_CLASSIFICATION = minio_utils.DataClassification.LAKE

DATA_PUBLIC_PREFIX = "data/public/"
DATA_RESTRICTED_PREFIX = "data/private/"

CT_MOBILE_METRICS = "data/private/ct_mobile_device_count_metrics.csv"
TIMESTAMP_COL = "timestamp"
HOURLY_METRIC_POLYGON_ID = "polygon_id"
UPTIME_COL_PREFIX = "Uptime_"
DOWNTIME_COL_PREFIX = "Downtime_"
DESCRIBE_COL_SUFFIXES = ["25%", "50%", "75%", "count", "max", "mean", "min", "std"]
PLOT_COL_SUFFIXES = ["50%"]

POLYGON_ID_COL = "id"

DEVICE_COUNT_THRESHOLD = 50
TEMPORAL_FILTER_WINDOW = pandas.Timedelta(days=28)

CT_VODACOM_POLYGONS = "ct_vodacom_polygons.geojson"
CT_HEX_L7_FILENAME = "city-hex-polygons-7.geojson"
CT_HEX_L8_FILENAME = "city-hex-polygons-8.geojson"
CT_WARD_FILENAME = "ct_wards.geojson"
CT_HEALTH_DISTRICT_FILENAME = "health_districts.geojson"

WARD_MOBILE_DATA_SUFFIX = "ward_mobile_count.geojson"
HEX_L7_MOBILE_DATA_SUFFIX = "hex_l7_mobile_count.geojson"
HEX_L8_MOBILE_DATA_SUFFIX = "hex_l8_mobile_count.geojson"
DISTRICT_MOBILE_DATA_SUFFIX = "district_mobile_count.geojson"
POLYGON_MOBILE_DATA_SUFFIX = "polygon_mobile_count.geojson"

CHOROPLETH_LAYERS = {
    WARD_MOBILE_DATA_SUFFIX,
    HEX_L7_MOBILE_DATA_SUFFIX,
    HEX_L8_MOBILE_DATA_SUFFIX,
    DISTRICT_MOBILE_DATA_SUFFIX,
    POLYGON_MOBILE_DATA_SUFFIX,
}

CHOROPLETH_SOURCE_LAYERS = {
    HEX_L7_MOBILE_DATA_SUFFIX: CT_HEX_L7_FILENAME,
    HEX_L8_MOBILE_DATA_SUFFIX: CT_HEX_L8_FILENAME,
    WARD_MOBILE_DATA_SUFFIX: CT_WARD_FILENAME,
    DISTRICT_MOBILE_DATA_SUFFIX: CT_HEALTH_DISTRICT_FILENAME,
    POLYGON_MOBILE_DATA_SUFFIX: CT_VODACOM_POLYGONS
}

LAYER_FILES = (
    (CT_VODACOM_POLYGONS, MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_RESTRICTED_PREFIX),
    (CT_HEX_L7_FILENAME, MINIO_LAKE_CLASSIFICATION, MINIO_HEX_BUCKET, ""),
    (CT_HEX_L8_FILENAME, MINIO_LAKE_CLASSIFICATION, MINIO_HEX_BUCKET, ""),
    (CT_WARD_FILENAME, MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    (CT_HEALTH_DISTRICT_FILENAME, MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
)

HEX_COUNT_INDEX_PROPERTY = "index"
CHOROPLETH_COL_LOOKUP = {
    # filename: (col name in gdf, sanitising function)
    WARD_MOBILE_DATA_SUFFIX: ("WardID", lambda ward: (str(int(ward)) if pandas.notna(ward) else None)),
    HEX_L7_MOBILE_DATA_SUFFIX: (HEX_COUNT_INDEX_PROPERTY, lambda hex: hex),
    HEX_L8_MOBILE_DATA_SUFFIX: (HEX_COUNT_INDEX_PROPERTY, lambda hex: hex),
    DISTRICT_MOBILE_DATA_SUFFIX: ("CITY_HLTH_RGN_NAME", lambda district: district.upper()),
    POLYGON_MOBILE_DATA_SUFFIX: ("id", lambda polygon: polygon),
}

NORM_PREFIX = "Norm"
DELTA_SUFFIX = "Delta_"
RELATIVE_DELTA_PREFIX = "RelativeDelta_"
RELATIVE_DELTA_PERCENT_PREFIX = "RelativeDeltaPercent_"

CASE_MAP_PREFIX = "widgets/private/case_count_maps/"


def get_mobile_data(minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_datafile:
        minio_utils.minio_to_file(
            filename=temp_datafile.name,
            minio_filename_override=CT_MOBILE_METRICS,
            minio_bucket=MINIO_COVID_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_EDGE_CLASSIFICATION,
        )

        mobile_data_df = pandas.read_csv(temp_datafile.name)

        for col in (TIMESTAMP_COL,):
            mobile_data_df[col] = pandas.to_datetime(mobile_data_df[col])

        mobile_data_df.set_index([TIMESTAMP_COL, HOURLY_METRIC_POLYGON_ID], inplace=True)
        mobile_data_df.sort_index(inplace=True)

    return mobile_data_df


def temporal_filter(mobile_data_df,
                    window_start=TEMPORAL_FILTER_WINDOW, window_end=None,
                    filter_start=None, filter_end=None):
    filter_start = mobile_data_df.index.get_level_values(TIMESTAMP_COL).max() if filter_start is None else filter_start
    filter_start = (filter_start - window_start) if window_start else filter_start

    filter_end = mobile_data_df.index.get_level_values(TIMESTAMP_COL).max() if filter_end is None else filter_end
    filter_end = (filter_end - window_end) if window_end else filter_end
    logging.debug(f"Temporal filter range: {filter_start} to {filter_end}")

    filter_slice = pandas.IndexSlice[filter_start:filter_end, :]

    return mobile_data_df.loc[filter_slice]


def calculate_delta_df(mobile_data_df):
    def calculate_date_delta(polygon_df):
        # Start off with the median uptime and downtime values for each polygon
        result_df = pandas.Series({**{
            UPTIME_COL_PREFIX + col: (
                polygon_df[UPTIME_COL_PREFIX + col].median()
                if polygon_df[UPTIME_COL_PREFIX + col].notna().any()
                else DEVICE_COUNT_THRESHOLD
            )
            for col in DESCRIBE_COL_SUFFIXES
        }, **{
            DOWNTIME_COL_PREFIX + col: (
                polygon_df[DOWNTIME_COL_PREFIX + col].median()
                if polygon_df[DOWNTIME_COL_PREFIX + col].notna().any()
                else DEVICE_COUNT_THRESHOLD
            )
            for col in DESCRIBE_COL_SUFFIXES
        }})

        # Initial delta calculation - uptime - downtime
        result_df = result_df.append(pandas.Series({
            DELTA_SUFFIX + col: result_df[UPTIME_COL_PREFIX + col] - result_df[DOWNTIME_COL_PREFIX + col]
            for col in DESCRIBE_COL_SUFFIXES
        }))

        # Relative delta increase - delta / downtime
        result_df = result_df.append(pandas.Series({
            RELATIVE_DELTA_PREFIX + col: result_df.loc[DELTA_SUFFIX + col] / result_df.loc[DOWNTIME_COL_PREFIX + col]
            for col in PLOT_COL_SUFFIXES
        }))

        # Percent increase - relative_delta*100, rounded off
        result_df = result_df.append(pandas.Series({
            RELATIVE_DELTA_PERCENT_PREFIX + col: (result_df.loc[RELATIVE_DELTA_PREFIX + col] * 100).round(0)
            for col in PLOT_COL_SUFFIXES
        }))

        return result_df

    delta_df = mobile_data_df.groupby(HOURLY_METRIC_POLYGON_ID).apply(calculate_date_delta)
    logging.debug(f"delta_df=\n{delta_df}")

    return delta_df


def delta_df_to_gdf(delta_df, polygon_gdf):
    delta_gdf = polygon_gdf.merge(
        delta_df,
        left_on=POLYGON_ID_COL, right_index=True,
        validate="1:1"
    )

    return delta_gdf


def remap_gdf(gdf1, gdf2, gdf1_cols):
    # Creating the source density GDF, with density measures
    source_gdf = gdf1.copy()
    for col in gdf1_cols:
        source_gdf[col] = gdf1[col] / gdf1.geometry.area

    remap_gdf = gdf2.copy()
    remap_gdf["RemapIndex"] = gdf2.index.to_series()

    # Getting the intersection between the two - basically the spatial outer join of the two GDFs
    source_gdf.sindex, remap_gdf.sindex
    intersection_gdf = geopandas.overlay(
        source_gdf.to_crs(remap_gdf.crs),
        remap_gdf,
        how='intersection'
    )

    # Function for finding the new value for each column
    def remap_func(group_gdf):
        weights = group_gdf.geometry.area

        new_vals = pandas.Series(
            {
                col: numpy.average(group_gdf[col], weights=weights)
                for col in gdf1_cols
            }
        )

        return new_vals

    # Aggregating on the new GDF's index
    aggregation_df = intersection_gdf.groupby(by="RemapIndex").apply(remap_func)
    aggregation_geometry = gdf2.geometry
    # Turning the density back to an absolute measure
    for col in gdf1_cols:
        aggregation_df[col] = (aggregation_df[col] * aggregation_geometry.area)

        if RELATIVE_DELTA_PERCENT_PREFIX in col:
            aggregation_df[col] = aggregation_df[col].round(0)

    new_gdf = geopandas.GeoDataFrame(aggregation_df, geometry=aggregation_geometry)
    new_gdf.index.name = gdf2.index.name

    return new_gdf


def normalise_gdf(data_gdf):
    # Normalised Values
    output_gdf = data_gdf.merge(
        pandas.DataFrame({**{
            NORM_PREFIX + UPTIME_COL_PREFIX + col: (data_gdf[UPTIME_COL_PREFIX + col]
                                                    / data_gdf[UPTIME_COL_PREFIX + col].max()).round(3)
            for col in PLOT_COL_SUFFIXES
        }, **{
            NORM_PREFIX + DOWNTIME_COL_PREFIX + col: (data_gdf[DOWNTIME_COL_PREFIX + col]
                                                      / data_gdf[DOWNTIME_COL_PREFIX + col].max()).round(3)
            for col in PLOT_COL_SUFFIXES
        }}, index=data_gdf.index),
        left_index=True, right_index=True, validate="one_to_one",
    )

    return output_gdf


def compress_gdf(delta_gdf):
    # Removing what we can
    logging.debug(f"(Before compression) delta_gdf.shape={delta_gdf.shape}")
    delta_gdf.dropna(how="all", inplace=True)
    logging.debug(f"(After compression) delta_gdf.shape={delta_gdf.shape}")

    return delta_gdf


def write_delta_gdf_to_disk(delta_gdf, tempdir, delta_filename):
    local_path = os.path.join(tempdir, delta_filename)
    delta_gdf.reset_index().to_file(local_path, driver='GeoJSON')

    return local_path, delta_gdf


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
                                                                                    LAYER_FILES,
                                                                                    secrets["minio"]["lake"]["access"],
                                                                                    secrets["minio"]["lake"]["secret"],)
        }
        logging.info("G[ot] layers")

        logging.info("G[etting] Mobile Data")
        mobile_df = get_mobile_data(secrets["minio"]["edge"]["access"],
                                    secrets["minio"]["edge"]["secret"])
        logging.info("G[ot] Mobile Data")

        logging.info("Temporally Filter[ing] data")
        filtered_df = temporal_filter(mobile_df)
        logging.info("Temporally Filter[ed] data ")

        logging.info("Calculat[ing] delta data")
        mobile_delta_df = calculate_delta_df(filtered_df)
        logging.info("Calculat[ed] delta data")

        logging.info("Spatialis[ing] delta data")
        _, vodacom_polygon_gdf = map_layers_dict[CT_VODACOM_POLYGONS]
        mobile_delta_gdf = delta_df_to_gdf(mobile_delta_df, vodacom_polygon_gdf)
        logging.info("Spatialis[ed] delta data")

        # Generating choropleths based upon delta gdf
        for layer_suffix in CHOROPLETH_LAYERS:
            layer_filename = f"{district_file_prefix}_{subdistrict_file_prefix}_{layer_suffix}"

            gdf_property, sanitise_func = CHOROPLETH_COL_LOOKUP[layer_suffix]
            logging.debug(f"gdf_property={gdf_property}")

            logging.info(f"Remapp[ing] cases for '{layer_filename}'")
            source_layer = CHOROPLETH_SOURCE_LAYERS[layer_suffix]
            _, target_gdf = map_layers_dict[source_layer]
            target_gdf.set_index(gdf_property, inplace=True)

            remapped_delta_gdf = remap_gdf(mobile_delta_gdf, target_gdf,
                                           [col for col in mobile_delta_df.columns if col != "geometry"]).reset_index()
            # Ugly hack to fix issue with indices
            remapped_delta_gdf.index.name = "dummy_index"

            logging.info(f"Remapp[ed] cases for '{layer_filename}'")

            logging.info("Normalis[ing] data")
            normalised_gdf = normalise_gdf(remapped_delta_gdf)
            logging.info("Normalis[ed] data")

            logging.info(f"Writ[ing] geojson for '{layer_filename}'")
            count_layer_values = write_delta_gdf_to_disk(normalised_gdf, tempdir, layer_filename)
            map_layers_dict[layer_filename] = count_layer_values
            logging.info(f"Wr[ote] geojson for '{layer_filename}'")

        logging.info("Writ[ing] layers to Minio")
        del map_layers_dict[CT_VODACOM_POLYGONS]
        city_map_layers_to_minio.write_layers_to_minio(map_layers_dict,
                                                       secrets["minio"]["edge"]["access"],
                                                       secrets["minio"]["edge"]["secret"])
        logging.info("Wr[ote] layers to Minio")
