import collections
import itertools
import json
import pprint
import logging
import os
import sys
import tempfile

import numpy

import city_map_widget_to_minio
import hotspot_map_widget_to_minio
import tree_layer_control
import service_delivery_latest_values_to_minio
from service_delivery_metric_map_layers_to_minio import (SD_METRIC_DATA_FILENAME, FEATURE_COL,
                                                         SERVICE_DELIVERY_MAP_PREFIX, HEX_METRIC_L7_FILENAME)

DATE_COL = "date"
BACKLOG_COL = "backlog"
SERVICE_STANDARD_COL = "service_standard"
LONG_BACKLOG_COL = "long_backlog"
LONG_BACKLOG_WEIGHTING_COL = "long_backlog_weighting"

DELTA_SUFFIX = "_delta"
RELATIVE_SUFFIX = "_relative"

HEX_COUNT_INDEX_PROPERTY = "index"

METRIC_COLS = (BACKLOG_COL, SERVICE_STANDARD_COL,)
METRIC_DELTA_COLS = (BACKLOG_COL + DELTA_SUFFIX + RELATIVE_SUFFIX, SERVICE_STANDARD_COL + DELTA_SUFFIX)

SERVICE_DELIVERY_LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    # Service Delivery Metrics
    ("Backlog (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, BACKLOG_COL,
         SERVICE_STANDARD_COL, LONG_BACKLOG_COL,
         LONG_BACKLOG_WEIGHTING_COL + DELTA_SUFFIX, DATE_COL),
        ("Hex ID", "Backlog",
         "Service Standard (%)", "Still Open > 180 days (%)",
         "Requests Opened since 2020-10-12", "Last Updated",),
        ("RdBu", True, [-numpy.inf, -516, -5, 5, 516, numpy.inf]), HEX_METRIC_L7_FILENAME, True, True, None, False
    )),
    ("Service Standard (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, SERVICE_STANDARD_COL,
         BACKLOG_COL, LONG_BACKLOG_COL,
         LONG_BACKLOG_WEIGHTING_COL + DELTA_SUFFIX, DATE_COL),
        ("Hex ID", "Service Standard (%)",
         "Backlog", "Still Open > 180 days (%)",
         "Requests Opened since 2020-10-12", "Last Updated",),
        ("RdBu", False, [-numpy.inf, 70, 80, 90, numpy.inf]), HEX_METRIC_L7_FILENAME, False, True, None, False
    )),
    ("Still Open > 180 days (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, LONG_BACKLOG_COL,
         SERVICE_STANDARD_COL, BACKLOG_COL,
         LONG_BACKLOG_WEIGHTING_COL + DELTA_SUFFIX, DATE_COL),
        ("Hex ID", "Still Open > 180 days (%)",
         "Service Standard (%)", "Backlog",
         "Requests Opened since 2020-10-12", "Last Updated",),
        ("RdBu", True, [-numpy.inf, 0, 0, 0.1, 1, 10, numpy.inf]), HEX_METRIC_L7_FILENAME, False, True, None, False
    )),
    ("Request Volume (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, LONG_BACKLOG_WEIGHTING_COL + DELTA_SUFFIX,
         LONG_BACKLOG_COL, BACKLOG_COL, SERVICE_STANDARD_COL, DATE_COL),
        ("Hex ID", "Requests Opened since 2020-10-12",
         "Still Open > 180 days (%)", "Backlog", "Service Standard (%)",
         "Last Updated",),
        ("Reds", False,), HEX_METRIC_L7_FILENAME, False, True, None, False
    )),
    # Service Delivery Metrics Change
    ("Backlog Change (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, BACKLOG_COL + DELTA_SUFFIX + RELATIVE_SUFFIX,
         SERVICE_STANDARD_COL + DELTA_SUFFIX, LONG_BACKLOG_COL + DELTA_SUFFIX,
         DATE_COL),
        ("Hex ID", "Backlog Growth (%)",
         "Service Standard Change (%)", "Still Open > 180 days Change (%)",
         "Last Updated",),
        ("RdBu", True, [-numpy.inf, -10, 10, numpy.inf]), HEX_METRIC_L7_FILENAME, False, True, None, False
    )),
    ("Service Standard Change (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, SERVICE_STANDARD_COL + DELTA_SUFFIX,
         BACKLOG_COL + DELTA_SUFFIX + RELATIVE_SUFFIX, LONG_BACKLOG_COL + DELTA_SUFFIX,
         DATE_COL),
        ("Hex ID", "Service Standard Change (%)",
         "Backlog Growth (%)", "Still Open > 180 days Change (%)",
         "Last Updated",),
        ("RdBu", False, [-numpy.inf, -1, 1, numpy.inf]), HEX_METRIC_L7_FILENAME, False, True, None, False
    )),
    ("Still Open > 180 days Change (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, LONG_BACKLOG_COL + DELTA_SUFFIX,
         SERVICE_STANDARD_COL + DELTA_SUFFIX, BACKLOG_COL + DELTA_SUFFIX + RELATIVE_SUFFIX,
         DATE_COL),
        ("Hex ID", "Still Open > 180 days Change (%)",
         "Service Standard Change (%)", "Backlog Growth (%)",
         "Last Updated",),
        ("RdBu", True, [-numpy.inf, -0.1, 0.1, numpy.inf]), HEX_METRIC_L7_FILENAME, False, True, None, False
    )),
))

SERVICE_DELIVERY_SHARED_LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    # Contextual Information
    ("ABSD Areas", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("ABSD_NAME",), ("ABSD Name",),
        ("black",), "absd_areas.geojson", False, False, None, False
    )),
    ("Subcouncils", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("SUB_CNCL_NAME",), ("Name",),
        ("black",), "subcouncils.geojson", False, False, None, False
    )),
    ("Wards", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("WardNo", "WardID"), ("Ward Number", "Ward ID"),
        ("black",), "ct_wards.geojson", False, False, None, False
    )),
    ("Official Suburbs", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("OFC_SBRB_NAME",), ("Official Suburb Name",),
        ("black",), "official_suburbs.geojson", False, False, None, False
    )),
    ("Special Rated Areas", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("SRA_NAME",), ("Name",),
        ("black",), "special_rated_areas.geojson", False, False, None, False
    )),
    ("Rental Stock (houses)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "Count",
         'House-Free Standing', 'House-Row House', 'House-Semi-Detached',
         'Maisonette-Row Maisonette', 'Maisonette-Semi-Detached',),
        ("Hex ID", "Number of Houses",
         'Free Standing Houses', 'Row Houses', 'Semi-Detached Houses',
         'Row Maisonettes', 'Semi-Detached Maisonettes',),
        ("Greys",), "city_house_counts.geojson", False, False, None, False
    )),
    ("Rental Stock (flats)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "Count", 'Flat', 'Hostel', 'Old Age Home'),
        ("Hex ID", "Total Blocks of Flats", 'Flats', 'Hostels', 'Old Age Homes'),
        ("Greys",), "city_flats_counts.geojson", False, False, None, False
    )),
    ("Rental Stock (hostels)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "Count",), ("Hex ID", "Number of Hostel Blocks",),
        ("Greys",), "city_hostel_counts.geojson", False, False, None, False
    )),
    ("Areas of Informality", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("AOI_NAME", "OTH_NAME", "AOI_TYPE"), ("Area Name", "Other Name", "Area Type",),
        ("grey",), "areas_of_informality_2019.geojson", False, False, None, False
    )),

    # Population Density
    ("2019 Population Estimate", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "PopDensity2019PerSqkm",), ("Hex ID", "People / km²",),
        ("Blues",), "sl_du_pop_est_2019_hex9.geojson", False, False, None, False
    )),
))

SERVICE_DELIVERY_METRICS = "SERVICE DELIVERY METRICS"
SERVICE_DELIVERY_METRICS_DELTA = "SERVICE DELIVERY METRICS CHANGE"
CONTEXTUAL_INFORMATION = "CONTEXTUAL INFORMATION"

CATEGORY_BUCKETS = [
    SERVICE_DELIVERY_METRICS,
    SERVICE_DELIVERY_METRICS_DELTA,
    CONTEXTUAL_INFORMATION,
]
CATEGORY_BUCKET_MAP = {
    # Service Delivery Metrics
    "Backlog (hexes)": SERVICE_DELIVERY_METRICS,
    "Service Standard (hexes)": SERVICE_DELIVERY_METRICS,
    "Still Open > 180 days (hexes)": SERVICE_DELIVERY_METRICS,
    "Request Volume (hexes)": SERVICE_DELIVERY_METRICS,

    # Service Delivery Metrics Delta
    "Backlog Change (hexes)": SERVICE_DELIVERY_METRICS_DELTA,
    "Service Standard Change (hexes)": SERVICE_DELIVERY_METRICS_DELTA,
    "Still Open > 180 days Change (hexes)": SERVICE_DELIVERY_METRICS_DELTA,

    # Contextual Information
    "ABSD Areas": CONTEXTUAL_INFORMATION,
    "Subcouncils": CONTEXTUAL_INFORMATION,
    "Wards": CONTEXTUAL_INFORMATION,
    "Special Rated Areas": CONTEXTUAL_INFORMATION,
    "Official Suburbs": CONTEXTUAL_INFORMATION,
    "Rental Stock (flats)": CONTEXTUAL_INFORMATION,
    "Rental Stock (houses)": CONTEXTUAL_INFORMATION,
    "Rental Stock (hostels)": CONTEXTUAL_INFORMATION,
    "Areas of Informality": CONTEXTUAL_INFORMATION,
    "2019 Population Estimate": CONTEXTUAL_INFORMATION,
}

BIN_QUANTILES = [0, 0.2, 0.4, 0.6, 0.8, 1]

MAP_ZOOM = 10
MAP_RIGHT_PADDING = 200
MINIMAP_WIDTH = 150
MINIMAP_PADDING = 20
MAP_FILENAME = "metric_map_widget.html"

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("G[etting] SD Data")
    sd_metric_data_df = service_delivery_latest_values_to_minio.get_data(
        SD_METRIC_DATA_FILENAME,
        secrets["minio"]["edge"]["access"],
        secrets["minio"]["edge"]["secret"]
    )
    feature_list = sd_metric_data_df[FEATURE_COL].unique()
    # hard coding this for now
    logging.debug(','.join(feature_list))
    feature_list = ["city",
                    "city-water_and_waste_services-water_and_sanitation",
                    "city-water_and_waste_services-solid_waste_management",
                    "city-energy_and_climate_change-electricity",
                    "city-transport-roads_infrastructure_and_management",
                    "city-human_settlements-public_housing",
                    "city-community_services_and_health-recreation_and_parks"]
    logging.info("G[ot] SD Data")

    # Has to be in the outer scope as the tempdir is used in multiple places
    with tempfile.TemporaryDirectory() as tempdir:
        logging.info("Fetch[ing] Folium dependencies")
        extra_js_tuple = [(tree_layer_control.TreeLayerControl._js_key, tree_layer_control.TreeLayerControl._js_link), ]
        extra_css_tuple = [
            (tree_layer_control.TreeLayerControl._css_key, tree_layer_control.TreeLayerControl._css_link),
        ]
        js_libs, css_libs = city_map_widget_to_minio.pull_out_leaflet_deps(tempdir,
                                                                           secrets["proxy"]["username"],
                                                                           secrets["proxy"]["password"],
                                                                           secrets["minio"]["edge"]["access"],
                                                                           secrets["minio"]["edge"]["secret"],
                                                                           extra_js_deps=extra_js_tuple,
                                                                           extra_css_deps=extra_css_tuple)
        logging.info("Fetch[ed] Folium dependencies")

        logging.info("G[etting] layers")
        shared_map_layers_dict = {
            # layername: (location, data, layer_metadata)
            layer: (local_path, layer_gdf, layer_metadata)
            for layer, local_path, layer_gdf, layer_metadata in
            city_map_widget_to_minio.get_layers(tempdir,
                                                secrets["minio"]["edge"]["access"],
                                                secrets["minio"]["edge"]["secret"],
                                                layer_properties=SERVICE_DELIVERY_SHARED_LAYER_PROPERTIES_LOOKUP,
                                                minio_path_prefix=SERVICE_DELIVERY_MAP_PREFIX)
        }
        logging.info("G[ot] layers")

        logging.info("G[etting] shared features")
        float_left_offset = f"{MINIMAP_WIDTH + MINIMAP_PADDING}px"
        shared_map_features = list(
            city_map_widget_to_minio.generate_map_features(shared_map_layers_dict,
                                                           layer_properties=SERVICE_DELIVERY_SHARED_LAYER_PROPERTIES_LOOKUP,
                                                           float_left_offset=float_left_offset,
                                                           choropleth_bins=BIN_QUANTILES)
        )
        logging.info("G[ot] shared features")

        logging.info("Generat[ing] base map features")
        base_map_features = list(hotspot_map_widget_to_minio.generate_base_map_features(
            tempdir,
            minimap=True, base_map_filename="subcouncils.geojson"
        ))
        logging.info("Generat[ed] base map features")

        for feature in feature_list:
            logging.info(f"Generat[ing] map widget for '{feature}' feature")

            logging.info("Generat[ing] feature specific features")
            feature_map_layers_dict = {
                # layername: (location, data, layer_metadata)
                layer: (local_path, layer_gdf, layer_metadata)
                for layer, local_path, layer_gdf, layer_metadata in
                city_map_widget_to_minio.get_layers(tempdir,
                                                    secrets["minio"]["edge"]["access"],
                                                    secrets["minio"]["edge"]["secret"],
                                                    layer_properties=SERVICE_DELIVERY_LAYER_PROPERTIES_LOOKUP,
                                                    minio_path_prefix=SERVICE_DELIVERY_MAP_PREFIX)
            }
            map_features = (
                city_map_widget_to_minio.generate_map_features(feature_map_layers_dict,
                                                               layer_properties=SERVICE_DELIVERY_LAYER_PROPERTIES_LOOKUP,
                                                               float_left_offset=float_left_offset,
                                                               choropleth_bins=BIN_QUANTILES,
                                                               properties_prefix=feature,
                                                               metadata_override=feature,
                                                               metadata_text_label_override="Requests not reflected since 2020-10-12")
            )
            logging.info("Generat[ed] feature specific features")

            logging.info("Generat[ing] map")
            map_feature_generator = itertools.chain(base_map_features,
                                                    shared_map_features,
                                                    map_features, )

            data_map = city_map_widget_to_minio.generate_map(map_feature_generator,
                                                             map_zoom=MAP_ZOOM,
                                                             map_right_padding=MAP_RIGHT_PADDING,
                                                             add_basemap=False)
            data_map = hotspot_map_widget_to_minio.add_tree_layer_control_to_map(data_map, CATEGORY_BUCKET_MAP)
            logging.info("Generat[ed] map")

            logging.info("Writ[ing] to Minio")
            city_map_widget_to_minio.write_map_to_minio(data_map, feature,
                                                        tempdir,
                                                        secrets["minio"]["edge"]["access"],
                                                        secrets["minio"]["edge"]["secret"],
                                                        js_libs, css_libs, MAP_FILENAME,
                                                        map_minio_prefix=SERVICE_DELIVERY_MAP_PREFIX)
            logging.info("Wr[ote] to Minio")
