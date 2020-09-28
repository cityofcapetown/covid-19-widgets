import json
import logging
import os
import pprint
import sys
import tempfile

from db_utils import minio_utils
import geopandas
import pandas
import shapely.geometry

import city_map_layers_to_minio

MINIO_COVID_BUCKET = "covid"
MINIO_HEX_BUCKET = "city-hex-polygons"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_PUBLIC_PREFIX = "data/public/"
DATA_RESTRICTED_PREFIX = "data/private/"
SERVICE_REQUEST_MAP_PREFIX = "widgets/private/business_continuity_service_request_map/"

SERVICE_REQUEST_BUCKET_NAME = "service-standards-tool.citizen-notifications"

WARD_COUNT_FILENAME = "ward_service_request_count.geojson"
SUBCOUNCIL_COUNT_FILENAME = "subcouncils_service_request_count.geojson"
HEX_COUNT_L7_FILENAME = "hex_l7_service_request_count.geojson"
HEX_COUNT_L8_FILENAME = "hex_l8_service_request_count.geojson"
CHOROPLETH_LAYERS = (
    WARD_COUNT_FILENAME,
    SUBCOUNCIL_COUNT_FILENAME,
    HEX_COUNT_L7_FILENAME,
    HEX_COUNT_L8_FILENAME,
)
CT_HEX_L7_FILENAME = "city-hex-polygons-7.geojson"
CT_HEX_L8_FILENAME = "city-hex-polygons-8.geojson"
HEX_INDEX_COL = "index"
CT_WARD_FILENAME = "ct_wards.geojson"
WARD_INDEX_COL = "WardID"
CT_SUBCOUNCIL_FILENAME = "subcouncils.geojson"
SC_INDEX_COL = "SUB_CNCL_NAME"

CHOROPLETH_SOURCE_ATTRS = {
    # filename: source_filename, index_col
    HEX_COUNT_L7_FILENAME: (CT_HEX_L7_FILENAME, HEX_INDEX_COL),
    HEX_COUNT_L8_FILENAME: (CT_HEX_L8_FILENAME, HEX_INDEX_COL),
    WARD_COUNT_FILENAME: (CT_WARD_FILENAME, WARD_INDEX_COL),
    SUBCOUNCIL_COUNT_FILENAME: (CT_SUBCOUNCIL_FILENAME, SC_INDEX_COL)
}

LAYER_FILES = (
    (CT_HEX_L7_FILENAME, MINIO_HEX_BUCKET, ""),
    (CT_HEX_L8_FILENAME, MINIO_HEX_BUCKET, ""),
    (CT_WARD_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    (CT_SUBCOUNCIL_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
)

SOD_DATE = pandas.Timestamp(year=2020, month=3, day=15, tz="Africa/Johannesburg")
LOWER_LIMIT = pandas.Timestamp(year=2019, month=7, day=1, tz="Africa/Johannesburg")

CREATION_TIMESTAMP_COL = "CreationTimestamp"
COMPLETION_TIMESTAMP_COL = "CompletionTimestamp"

TIMESTAMP_COLS = [CREATION_TIMESTAMP_COL, "ModificationTimestamp", COMPLETION_TIMESTAMP_COL]
TIME_PERIODS = (
    # (prefix, date gen func)
    ("last_day", lambda sr_df: sr_df[TIMESTAMP_COLS].dropna(axis=1).max().max().date() - pandas.Timedelta(days=1)),
    ("last_week", lambda sr_df: sr_df[TIMESTAMP_COLS].dropna(axis=1).max().max().date() - pandas.Timedelta(days=7)),
    ("last_month", lambda sr_df: sr_df[TIMESTAMP_COLS].dropna(axis=1).max().max().date() - pandas.Timedelta(days=28)),
    ("since_sod", lambda _: SOD_DATE.date())
)

LATITUDE_COL = "Latitude"
LONGITUDE_COL = "Longitude"

DURATION_COL = "Duration"

OPENED_COL = "Opened"
CLOSED_COL = "Closed"
NETT_OPENED_COL = "NettOpened"
P10_DURATION_COL = "P10"
P50_DURATION_COL = "P50"
P80_DURATION_COL = "P80"

METADATA_DELTA_SUFFIX = "delta"
METADATA_RELATIVE_DELTA_SUFFIX = "relative_delta"
METADATA_DELTA_LENGTH = pandas.Timedelta(days=7)

METADATA_OPENED_TOTAL = "opened_total"
METADATA_CLOSED_TOTAL = "closed_total"
METADATA_OPENED_NON_SPATIAL = "opened_non_spatial"
METADATA_CLOSED_NON_SPATIAL = "opened_non_spatial"
METADATA_P10 = "p10_total"
METADATA_P50 = "p50_total"
METADATA_P80 = "p80_total"


def get_service_request_data(minio_access, minio_secret):
    service_request_df = minio_utils.minio_to_dataframe(
        minio_bucket=SERVICE_REQUEST_BUCKET_NAME,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=minio_utils.DataClassification.CONFIDENTIAL,
        use_cache=True
    )

    return service_request_df


def filter_sr_data(sr_df, start_date, directorate=None, spatial_filter=False, open_filter=False):
    logging.debug(f"sr_df.shape={sr_df.shape}")
    # Date filtering
    filter_string = (f"(({CREATION_TIMESTAMP_COL}.dt.date >= @start_date) | "
                     f"({COMPLETION_TIMESTAMP_COL}.dt.date >= @start_date))")

    # Directorate
    if directorate:
        directorate_filter_str = directorate.lower()
        filter_string += " & (directorate.str.lower() == @directorate_filter_str)"

    # Spatial Filter
    if spatial_filter:
        filter_string += " & (Latitude.notna() & Longitude.notna())"

    # Open Filter
    if open_filter:
        filter_string += f" & ({COMPLETION_TIMESTAMP_COL}.isna() & ({CREATION_TIMESTAMP_COL} >= @LOWER_LIMIT))"

    logging.debug(f"Resulting filter string: '{filter_string}'")
    filter_df = sr_df.query(filter_string)

    logging.debug(f"filter_df.shape={filter_df.shape}")
    logging.debug(f"filter_df.head(10)={filter_df.head(10)}")

    return filter_df


def spatialise_df(sr_df, spatial_layer):
    logging.debug(f"sr_df.shape={sr_df.shape}")
    sr_df = sr_df.query(f"{LATITUDE_COL}.notna() & {LONGITUDE_COL}.notna()")

    sr_gdf = geopandas.GeoDataFrame(
        sr_df,
        geometry=sr_df.apply(
            lambda row: shapely.geometry.Point(row.loc[LONGITUDE_COL], row.loc[LATITUDE_COL]),
            axis=1
        ),
        crs="epsg:4326"
    )

    sr_gdf = geopandas.sjoin(
        spatial_layer, sr_gdf, op='intersects', how="inner"
    )
    logging.debug(f"sr_gdf.shape={sr_gdf.shape}")

    return sr_gdf


def _get_calc_sr_dfs(sr_df, start_date, end_date=None):
    if end_date is None:
        end_date = sr_df[[CREATION_TIMESTAMP_COL, COMPLETION_TIMESTAMP_COL]].max().max().date()

    opened_end_date_string = f"({CREATION_TIMESTAMP_COL}.dt.date <= @end_date)"
    opened_df = sr_df.query(
        f"({CREATION_TIMESTAMP_COL}.dt.date >= @start_date) & {opened_end_date_string}"
    )

    closed_end_date_string = f"({COMPLETION_TIMESTAMP_COL}.dt.date <= @end_date)"
    closed_df = sr_df.query(
        f"({COMPLETION_TIMESTAMP_COL}.dt.date >= @start_date) & {closed_end_date_string}"
    )
    all_df = sr_df.query(
        f"("
        f"({CREATION_TIMESTAMP_COL}.dt.date >= @start_date) & {opened_end_date_string}"
        f") | ("
        f"({COMPLETION_TIMESTAMP_COL}.dt.date >= @start_date) & {closed_end_date_string}"
        f") | ("
        f"{COMPLETION_TIMESTAMP_COL}.isna() & ({CREATION_TIMESTAMP_COL} >= @LOWER_LIMIT) & {opened_end_date_string}"
        f")"
    )

    return opened_df, closed_df, all_df


def count_srs(start_date, sr_gdf, spatial_index_col, time_period_prefix):
    opened_gdf, closed_gdf, all_gdf = _get_calc_sr_dfs(sr_gdf, start_date)

    count_df = pandas.DataFrame({
        f"{OPENED_COL}_{time_period_prefix}":
            opened_gdf.assign(**{OPENED_COL: 1})[[spatial_index_col, OPENED_COL]].groupby(
                by=[spatial_index_col]
            ).sum()[OPENED_COL],
        f"{CLOSED_COL}_{time_period_prefix}":
            closed_gdf.assign(**{CLOSED_COL: 1})[[spatial_index_col, CLOSED_COL]].groupby(
                by=[spatial_index_col]
            ).sum()[CLOSED_COL],
        f"{P10_DURATION_COL}_{time_period_prefix}": (all_gdf.groupby(
            by=[spatial_index_col]
        )[DURATION_COL].quantile(0.1) / 3600 / 24).round(1),
        f"{P50_DURATION_COL}_{time_period_prefix}": (all_gdf.groupby(
            by=[spatial_index_col]
        )[DURATION_COL].quantile(0.5) / 3600 / 24).round(1),
        f"{P80_DURATION_COL}_{time_period_prefix}": (all_gdf.groupby(
            by=[spatial_index_col]
        )[DURATION_COL].quantile(0.8) / 3600 / 24).round(1),
    })

    return count_df


def count_sr_data(sr_df, spatial_layer, spatial_index_col, start_dates):
    sr_gdf = spatialise_df(sr_df, spatial_layer)

    count_dfs = (
        count_srs(start_date, sr_gdf, spatial_index_col, time_period_label)
        for time_period_label, start_date in start_dates
    )

    count_gdf = geopandas.GeoDataFrame(
        pandas.concat(count_dfs, axis=1),
        geometry=spatial_layer.set_index(spatial_index_col).geometry,
        crs="epsg:4326"
    )
    # Filling NAs
    for col in count_gdf.columns:
        if col.startswith(OPENED_COL) or col.startswith(CLOSED_COL):
            count_gdf[col].fillna(0, inplace=True)

    # Calculating nett open values
    for time_period_label, _ in start_dates:
        count_gdf[f"{NETT_OPENED_COL}_{time_period_label}"] = (
                count_gdf[f"{OPENED_COL}_{time_period_label}"] - count_gdf[f"{CLOSED_COL}_{time_period_label}"]
        )

    logging.debug(f"count_gdf.columns={count_gdf.columns}")
    logging.debug(f"count_gdf.head(10)=\n{count_gdf.head(10)}")

    return count_gdf


def _calc_metadata(sr_df, start_date, end_date=None):
    opened_df, closed_df, all_df = _get_calc_sr_dfs(sr_df, start_date, end_date)

    metadata_calc_dict = {
        f"{METADATA_OPENED_TOTAL}": opened_df.shape[0],
        f"{METADATA_CLOSED_TOTAL}": closed_df.shape[0],
        f"{METADATA_OPENED_NON_SPATIAL}": (opened_df[LATITUDE_COL].isna() | opened_df[LONGITUDE_COL].isna()).sum(),
        f"{METADATA_CLOSED_NON_SPATIAL}": (closed_df[LATITUDE_COL].isna() | closed_df[LONGITUDE_COL].isna()).sum(),
        f"{METADATA_P10}": (all_df[DURATION_COL].quantile(0.1) / 3600 / 24).round(1),
        f"{METADATA_P50}": (all_df[DURATION_COL].quantile(0.5) / 3600 / 24).round(1),
        f"{METADATA_P80}": (all_df[DURATION_COL].quantile(0.8) / 3600 / 24).round(1),
    }

    return metadata_calc_dict


def generate_sr_time_period_metadata(sr_df, start_date, time_period_suffix):
    current_metadata_dict = {
        f"{k}_{time_period_suffix}": v
        for k, v in _calc_metadata(sr_df, start_date).items()
    }
    logging.debug(f"current_metadata_dict=\n{pprint.pformat(current_metadata_dict)}")

    delta_start_date = start_date - METADATA_DELTA_LENGTH
    delta_end_date = sr_df[[CREATION_TIMESTAMP_COL, COMPLETION_TIMESTAMP_COL]].max().max().date() - METADATA_DELTA_LENGTH

    delta_metadata_dict = {
        f"{k}_{time_period_suffix}_{METADATA_DELTA_SUFFIX}": current_metadata_dict[f"{k}_{time_period_suffix}"] - v
        for k, v in _calc_metadata(sr_df, delta_start_date, delta_end_date).items()
    }
    logging.debug(f"delta_metadata_dict=\n{pprint.pformat(delta_metadata_dict)}")

    relative_delta_metadata_dict = {
        f"{k}_{METADATA_RELATIVE_DELTA_SUFFIX}": (
                round(delta_metadata_dict[f"{k}_{METADATA_DELTA_SUFFIX}"] / v, 4) if v else None
        )
        for k, v in current_metadata_dict.items()
    }
    logging.debug(f"delta_metadata_dict=\n{pprint.pformat(delta_metadata_dict)}")

    metadata_dict = {
        **current_metadata_dict,
        **delta_metadata_dict,
        **relative_delta_metadata_dict
    }
    metadata_dict = {
        k: str(v)
        for k, v in metadata_dict.items()
    }

    return metadata_dict


def generate_sr_metadata(sr_df, start_dates):
    metadata_dicts = (
        generate_sr_time_period_metadata(sr_df, start_date, time_period_label)
        for time_period_label, start_date in start_dates
    )

    metadata_dict = {}
    for md in metadata_dicts:
        metadata_dict = {**metadata_dict, **md}

    logging.debug(f"metadata_dict=\n{pprint.pformat(metadata_dict)}")

    return metadata_dict


def write_service_request_count_gdf_to_disk(case_count_data_gdf, tempdir, case_count_filename):
    local_path = os.path.join(tempdir, case_count_filename)
    case_count_data_gdf.reset_index().to_file(local_path, driver='GeoJSON')

    return local_path, case_count_data_gdf


def write_metadata_to_minio(metadata_dict, tempdir, metadata_filename, minio_access, minio_secret):
    local_path = os.path.join(tempdir, metadata_filename)
    with open(local_path, "w") as metadata_file:
        json.dump(metadata_dict, metadata_file)

    result = minio_utils.file_to_minio(
        filename=local_path,
        filename_prefix_override=SERVICE_REQUEST_MAP_PREFIX,
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

    directorate_file_prefix = sys.argv[1]
    directorate_title = sys.argv[2]

    logging.info(f"Generat[ing] map layers for '{directorate_title}'")

    # Has to be in the outer scope as use the tempdir in multiple places
    with tempfile.TemporaryDirectory() as tempdir:
        logging.info("G[etting] layers")
        map_layers_dict = {
            layer: (local_path, layer_gdf)
            for layer, local_path, layer_gdf in city_map_layers_to_minio.get_layers(tempdir,
                                                                                    secrets["minio"]["edge"]["access"],
                                                                                    secrets["minio"]["edge"]["secret"],
                                                                                    LAYER_FILES)
        }
        logging.info("G[ot] layers")

        logging.info("G[etting] SR Data")
        sr_data_df = get_service_request_data(secrets["minio"]["confidential"]["access"],
                                              secrets["minio"]["confidential"]["secret"])
        logging.info("G[ot] SR Data")

        logging.info("Upfront filter[ing] of SR Data")
        filter_df = filter_sr_data(
            sr_data_df, SOD_DATE,
            directorate_title if directorate_title != "*" else None
        )
        logging.info("Upfront filter[ed] SR Data")

        logging.info("Determin[ing] dates for counting")
        time_period_start_dates = list(map(
            lambda time_period_tuple: (time_period_tuple[0], time_period_tuple[1](sr_data_df)),
            TIME_PERIODS
        ))
        logging.debug(f"start_dates=\n{pprint.pformat(time_period_start_dates)}")
        logging.info("Determin[ed] dates for counting")

        for layer_filename in CHOROPLETH_LAYERS:
            source_layer, source_layer_index = CHOROPLETH_SOURCE_ATTRS[layer_filename]
            _, data_gdf = map_layers_dict[source_layer]

            directorate_layer_filename = f'{directorate_file_prefix}_{layer_filename}'
            logging.info(f"Count[ing] requests for '{directorate_layer_filename}'")
            request_count_gdf = count_sr_data(
                filter_df, data_gdf, source_layer_index, time_period_start_dates
            )
            logging.info(f"Count[ed] requests for '{directorate_layer_filename}'")

            logging.info(f"Writ[ing] geojson for '{directorate_layer_filename}'")
            sr_count_layer_values = write_service_request_count_gdf_to_disk(request_count_gdf,
                                                                            tempdir, directorate_layer_filename)
            map_layers_dict[directorate_layer_filename] = sr_count_layer_values
            logging.info(f"Wr[ote] geojson for '{directorate_layer_filename}'")

            logging.info(f"Writ[ing] metadata for '{directorate_layer_filename}'")
            layer_metadata = generate_sr_metadata(filter_df, time_period_start_dates)
            logging.debug(f"layer_metadata=\n{pprint.pformat(layer_metadata)}")
            layer_stem, layer_ext = os.path.splitext(directorate_layer_filename)
            metadata_filename = layer_stem + ".json"
            write_metadata_to_minio(layer_metadata, tempdir, metadata_filename,
                                    secrets["minio"]["edge"]["access"],
                                    secrets["minio"]["edge"]["secret"])
            logging.info(f"Wr[ote] metadata for '{directorate_layer_filename}'")

        logging.info("Writ[ing] layers to Minio")
        city_map_layers_to_minio.write_layers_to_minio(map_layers_dict,
                                                       secrets["minio"]["edge"]["access"],
                                                       secrets["minio"]["edge"]["secret"],
                                                       prefix=SERVICE_REQUEST_MAP_PREFIX)
        logging.info("Wr[ote] layers to Minio")
