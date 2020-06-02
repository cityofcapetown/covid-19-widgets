import json
import logging
import os
import pprint
import sys
import tempfile

from db_utils import minio_utils
import geopandas
from h3 import h3
import pandas
import pytz

MINIO_COVID_BUCKET = "covid"
MINIO_HEX_BUCKET = "city-hex-polygons"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_PUBLIC_PREFIX = "data/public/"
DATA_RESTRICTED_PREFIX = "data/private/"
SERVICE_REQUEST_MAP_PREFIX = "business_continuity_service_request_map/"
WIDGETS_RESTRICTED_PREFIX = "widgets/private/"

SERVICE_REQUEST_BUCKET_NAME = "service-standards-tool.citizen-notifications"

WARD_COUNT_FILENAME = "ward_service_request_count.geojson"
HEX_COUNT_FILENAME = "hex_l7_service_request_count.geojson"
CHOROPLETH_LAYERS = (
    # WARD_COUNT_FILENAME,
    HEX_COUNT_FILENAME,
)
CT_HEX_L7_FILENAME = "city-hex-polygons-7.geojson"
CT_WARD_FILENAME = "ct_wards.geojson"
CHOROPLETH_SOURCE_ATTRS = {
    # filename: source_filename, hex_level
    HEX_COUNT_FILENAME: (CT_HEX_L7_FILENAME, 7)
    # WARD_COUNT_FILENAME: CT_WARD_FILENAME
}

LAYER_FILES = (
    ("informal_settlements.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("health_care_facilities.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    (CT_HEX_L7_FILENAME, MINIO_HEX_BUCKET, ""),
    # (CT_WARD_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
)

SOD_DATE = pandas.Timestamp(year=2020, month=3, day=15)

CREATION_TIMESTAMP_COL = "CreationTimestamp"
COMPLETION_TIMESTAMP_COL = "CompletionTimestamp"

TIMESTAMP_COLS = [CREATION_TIMESTAMP_COL, "ModificationTimestamp", COMPLETION_TIMESTAMP_COL]
TIME_PERIODS = (
    # (prefix, date gen func)
    ("last_day", lambda sr_df: sr_df[TIMESTAMP_COLS].dropna(axis=1).max().max().date() - pandas.Timedelta(days=1)),
    ("last_week", lambda sr_df: sr_df[TIMESTAMP_COLS].dropna(axis=1).max().max().date() - pandas.Timedelta(days=7)),
    ("last_month", lambda sr_df: sr_df[TIMESTAMP_COLS].dropna(axis=1).max().max().date() - pandas.Timedelta(days=28)),
    ("since_sod", lambda _: SOD_DATE)
)

LATITUDE_COL = "Latitude"
LONGITUDE_COL = "Longitude"

HEX_LEVELS = (7,)
HEX_INDEX_COL = "index"

OPENED_COL = "Opened"
CLOSED_COL = "Closed"
NETT_OPENED_COL = "NettOpened"

METADATA_OPENED_TOTAL = "opened_total"
METADATA_CLOSED_TOTAL = "closed_total"
METADATA_OPENED_NON_SPATIAL = "opened_non_spatial"
METADATA_CLOSED_NON_SPATIAL = "opened_non_spatial"


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
    filter_string = f"(({CREATION_TIMESTAMP_COL}.dt.date >= @start_date) | ({COMPLETION_TIMESTAMP_COL}.dt.date >= @start_date))"

    # Directorate
    if directorate is not None:
        directorate_filter_str = directorate.lower()
        filter_string += " & (directorate.str.lower() == @directorate_filter_str)"

    # Spatial Filter
    if spatial_filter:
        filter_string += " & (Latitude.notna() & Longitude.notna())"

    # Open Filter
    if open_filter:
        filter_string += f" & ({COMPLETION_TIMESTAMP_COL}.isna())"

    logging.debug(f"Resulting filter string: '{filter_string}'")
    filter_df = sr_df.query(filter_string)

    logging.debug(f"filter_df.shape={filter_df.shape}")
    logging.debug(f"filter_df.head(10)={filter_df.head(10)}")

    return filter_df


def apply_hexes(sr_df, hex_level):
    return sr_df.apply(
        lambda row: h3.geo_to_h3(row.Latitude, row.Longitude, resolution=hex_level),
        axis="columns"
    )


def count_sr_data(sr_df, start_date, hex_gdf, hex_level):
    sr_df["hex_index"] = apply_hexes(sr_df, hex_level)

    hex_count_gdf = hex_gdf.set_index(HEX_INDEX_COL).assign(**{
        OPENED_COL: sr_df.query(
            f"{CREATION_TIMESTAMP_COL}.dt.date >= @start_date"
        ).assign(**{OPENED_COL: 1}).groupby(by=["hex_index"]).sum()[OPENED_COL],
        CLOSED_COL: sr_df.query(
            f"{COMPLETION_TIMESTAMP_COL}.dt.date >= @start_date"
        ).assign(**{CLOSED_COL: 1}).groupby(by=["hex_index"]).sum()[CLOSED_COL]
    })
    for col in [OPENED_COL, CLOSED_COL]:
        hex_count_gdf[col].fillna(0, inplace=True)

    hex_count_gdf[NETT_OPENED_COL] = hex_count_gdf[OPENED_COL] - hex_count_gdf[CLOSED_COL]

    return hex_count_gdf


def generate_sr_metadata(sr_df, start_date):
    opened_df = sr_df.query(f"{CREATION_TIMESTAMP_COL}.dt.date >= @start_date")
    closed_df = sr_df.query(f"{COMPLETION_TIMESTAMP_COL}.dt.date >= @start_date")

    metadata_dict = {
        METADATA_OPENED_TOTAL: str(opened_df.shape[0]),
        METADATA_CLOSED_TOTAL: str(closed_df.shape[0]),
        METADATA_OPENED_NON_SPATIAL: str((opened_df[LATITUDE_COL].isna() | opened_df[LONGITUDE_COL].isna()).sum()),
        METADATA_CLOSED_NON_SPATIAL: str((closed_df[LATITUDE_COL].isna() | closed_df[LONGITUDE_COL].isna()).sum())
    }
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
        filename_prefix_override=WIDGETS_RESTRICTED_PREFIX + SERVICE_REQUEST_MAP_PREFIX,
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
            filename_prefix_override=WIDGETS_RESTRICTED_PREFIX + SERVICE_REQUEST_MAP_PREFIX,
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
            for layer, local_path, layer_gdf in get_layers(tempdir,
                                                           secrets["minio"]["edge"]["access"],
                                                           secrets["minio"]["edge"]["secret"])
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

        for layer_filename in CHOROPLETH_LAYERS:
            source_layer, hex_level = CHOROPLETH_SOURCE_ATTRS[layer_filename]
            _, data_gdf = map_layers_dict[source_layer]

            for time_period_prefix, time_period_date_func in TIME_PERIODS:
                time_period_start_date = time_period_date_func(filter_df)
                logging.debug(f"time_period_start_date={time_period_start_date.strftime('%Y-%m-%d')}")

                time_period_layer_filename = f'{time_period_prefix}_{directorate_file_prefix}_{layer_filename}'
                logging.debug(f"time_period_layer_filename={time_period_layer_filename}")

                logging.info(f"Count[ing] requests for '{time_period_layer_filename}'")

                logging.info(f"Filter[ing] for '{time_period_layer_filename}'")
                time_period_filtered_df = filter_sr_data(filter_df, time_period_start_date)
                logging.info(f"Filter[ed] for '{time_period_layer_filename}'")

                logging.info(f"Count[ing] requests for '{time_period_layer_filename}'")
                request_count_gdf = count_sr_data(
                    time_period_filtered_df, time_period_start_date, data_gdf, hex_level
                )
                logging.info(f"Count[ed] requests for '{time_period_layer_filename}'")

                logging.info(f"Writ[ing] geojson for '{time_period_layer_filename}'")
                sr_count_layer_values = write_service_request_count_gdf_to_disk(request_count_gdf,
                                                                                tempdir, time_period_layer_filename)
                map_layers_dict[time_period_layer_filename] = sr_count_layer_values
                logging.info(f"Wr[ote] geojson for '{time_period_layer_filename}'")

                logging.info(f"Writ[ing] metadata for '{time_period_layer_filename}'")
                layer_metadata = generate_sr_metadata(time_period_filtered_df, time_period_start_date)
                logging.debug(f"layer_metadata=\n{pprint.pformat(layer_metadata)}")
                layer_stem, layer_ext = os.path.splitext(time_period_layer_filename)
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
