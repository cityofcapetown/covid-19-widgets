import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils

import city_map_layers_to_minio
import epi_map_case_layers_to_minio

MINIO_COVID_BUCKET = "covid"
MINIO_EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_PUBLIC_PREFIX = "data/public/"
DATA_RESTRICTED_PREFIX = "data/private/"
SERVICE_DELIVERY_MAP_PREFIX = "widgets/private/business_continuity_service_request_map/"
SERVICE_DELIVERY_METRIC_MAP_PREFIX = "widgets/private/business_continuity_service_delivery_map/"
DATA_VULNERABILITY_PREFIX = "data/staging/vulenerability_layers/"

LAYER_FILES = (
    ("health_care_facilities.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("ct_wards.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("health_districts.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("public_transport_interchanges.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("city_house_counts.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("city_flats_counts.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("city_hostel_counts.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("areas_of_informality_2019.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("cpop_gt55.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("official_suburbs.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("official_suburb_labels.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("ct_roads.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("ct_railways.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("absd_areas.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("subcouncils.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("ct_wards.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("special_rated_areas.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    (f"city_all_{epi_map_case_layers_to_minio.HEX_L8_COUNT_SUFFIX}", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, city_map_layers_to_minio.CASE_MAP_PREFIX),
    (f"city_all_{epi_map_case_layers_to_minio.DISTRICT_COUNT_SUFFIX}", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, city_map_layers_to_minio.CASE_MAP_PREFIX),
    (f"city_all_{epi_map_case_layers_to_minio.HEX_L8_COUNT_SUFFIX}".replace(".geojson", ".json"), MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, city_map_layers_to_minio.CASE_MAP_PREFIX),
    (f"city_all_{epi_map_case_layers_to_minio.DISTRICT_COUNT_SUFFIX}".replace(".geojson", ".json"), MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, city_map_layers_to_minio.CASE_MAP_PREFIX),
    ("city-absence-counts-hex.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_RESTRICTED_PREFIX),
    ("sl_du_pop_est_2019_hex9.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("sewer_pump_stations.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("sewer_drainage_areas.geojson", MINIO_EDGE_CLASSIFICATION, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    # Has to be in the outer scope as the tempdir is used in multiple places
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

        logging.info("Writ[ing] layers to Minio")
        city_map_layers_to_minio.write_layers_to_minio(map_layers_dict,
                                                       secrets["minio"]["edge"]["access"],
                                                       secrets["minio"]["edge"]["secret"],
                                                       prefix=SERVICE_DELIVERY_MAP_PREFIX)
        city_map_layers_to_minio.write_layers_to_minio(map_layers_dict,
                                                       secrets["minio"]["edge"]["access"],
                                                       secrets["minio"]["edge"]["secret"],
                                                       prefix=SERVICE_DELIVERY_METRIC_MAP_PREFIX)
        logging.info("Wr[ote] layers to Minio")
