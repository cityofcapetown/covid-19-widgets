import json
import logging
import os
import pandas
import sys
import tempfile

from db_utils import minio_utils
import geopandas

MINIO_COVID_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_PUBLIC_PREFIX = "data/public/"
DATA_RESTRICTED_PREFIX = "data/private/"
DATA_COMMUNITY_RESPONSE_PREFIX = "data/staging/community_response/"
CASE_MAP_PREFIX = "widgets/private/case_count_maps/"
DATA_VULNERABILITY_PREFIX = "data/staging/vulenerability_layers/"

MINIO_COD_BUCKET = "community-organisation-database.community-organisations"

CT_WARD_FILENAME = "ct_wards.geojson"
CT_HEALTH_DISTRICT_FILENAME = "health_districts.geojson"

LAYER_FILES = (
    ("informal_settlements.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("health_care_facilities.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    (CT_WARD_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    (CT_HEALTH_DISTRICT_FILENAME, MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("wcpg_testing_facilities.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("sl_du_pop_est_2019_hex9.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("province_sevi_v2_hex9.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
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
    ("combined_senior_citizens_layer.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("cpop_gt55.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("employment_density_survey_20200515.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
    ("official_suburbs.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("official_suburb_labels.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("coct_cbt_v2.geojson", MINIO_COVID_BUCKET, DATA_COMMUNITY_RESPONSE_PREFIX),
    ("ct_cans.geojson", MINIO_COVID_BUCKET, DATA_COMMUNITY_RESPONSE_PREFIX),
    ("npo_publish_data.geojson", MINIO_COVID_BUCKET, DATA_COMMUNITY_RESPONSE_PREFIX),
    ("ct_roads.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("ct_railways.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("community-organisations-environment.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-safety-and-security-organisations.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-sports.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-civic-based-organisations.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-business.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-youth.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-arts-and-culture.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-education.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-faith-based-organisations.geojson", MINIO_COD_BUCKET, ""),
    ("community-organisations-designated-vulnerable-groups.geojson", MINIO_COD_BUCKET, ""),
    ("absd_areas.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("subcouncils.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("ct_wards.geojson", MINIO_COVID_BUCKET, DATA_PUBLIC_PREFIX),
    ("special_rated_areas.geojson", MINIO_COVID_BUCKET, DATA_VULNERABILITY_PREFIX),
)


def get_layers(tempdir, minio_access, minio_secret, layers=LAYER_FILES):
    for layer, layer_bucket, layer_minio_prefix in layers:
        local_path = os.path.join(tempdir, layer)

        minio_utils.minio_to_file(
            filename=local_path,
            minio_filename_override=layer_minio_prefix + layer,
            minio_bucket=layer_bucket,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        read_df_func = geopandas.read_file if local_path.endswith(".geojson") else pandas.read_json
        layer_gdf = read_df_func(local_path)

        yield layer, local_path, layer_gdf


def write_layers_to_minio(layers_dict, minio_access, minio_secret, prefix=CASE_MAP_PREFIX):
    for layer_name, (layer_local_path, _) in layers_dict.items():
        result = minio_utils.file_to_minio(
            filename=layer_local_path,
            filename_prefix_override=prefix,
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

        logging.info("Writ[ing] layers to Minio")
        write_layers_to_minio(map_layers_dict,
                              secrets["minio"]["edge"]["access"],
                              secrets["minio"]["edge"]["secret"])
        logging.info("Wr[ote] layers to Minio")
