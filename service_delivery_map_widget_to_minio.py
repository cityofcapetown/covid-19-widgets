import collections
import itertools
import json
import logging
import os
import sys
import tempfile

import epi_map_case_layers_to_minio
import city_map_widget_to_minio
import hotspot_map_widget_to_minio
import service_request_map_layers_to_minio
import tree_layer_control

HEX_COUNT_INDEX_PROPERTY = "index"
DISTRICT_NAME_PROPERTY = "CITY_HLTH_RGN_NAME"
SC_COUNT_INDEX_PROPERTY = "SUB_CNCL_NAME"

SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX = "_last_month"

SERVICE_DELIVERY_LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    # Pandemic Information
    ("Active Covid-19 Cases (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY,
         epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL,
         epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL + epi_map_case_layers_to_minio.DELTA_SUFFIX),
        ("Hex ID", "Presumed Active Cases", "Change in Presumed Active Cases"),
        ("Reds",), "city_all_" + epi_map_case_layers_to_minio.HEX_L8_COUNT_SUFFIX, True, True,
        epi_map_case_layers_to_minio.ACTIVE_METADATA_KEY, False
    )),
    ("Active Covid-19 Cases (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY,
         epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL,
         epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL + epi_map_case_layers_to_minio.DELTA_SUFFIX),
        ("Healthcare District Name", "Presumed Active Cases", "Change in Presumed Active Cases"),
        ("Reds",), "city_all_" + epi_map_case_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.ACTIVE_METADATA_KEY, False
    )),
    ("Active Covid-19 Cases Change (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY,
         epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL + epi_map_case_layers_to_minio.DELTA_SUFFIX,
         epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Healthcare District Name", "Change in Presumed Active Cases", "Presumed Active Cases"),
        ("YlOrRd",), "city_all_" + epi_map_case_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.ACTIVE_METADATA_KEY, False
    )),
    ("Covid-19 Mortality (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, epi_map_case_layers_to_minio.DEATHS_COUNT_COL), ("Hex ID", "Deaths"),
        ("Greys",), "city_all_" + epi_map_case_layers_to_minio.HEX_L8_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.DEATHS_METADATA_KEY, False
    )),
    ("Covid-19 Mortality (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY,
         epi_map_case_layers_to_minio.DEATHS_COUNT_COL,
         epi_map_case_layers_to_minio.DEATHS_COUNT_COL + epi_map_case_layers_to_minio.DELTA_SUFFIX),
        ("Healthcare District Name", "Deaths", "Increase in Deaths"),
        ("Greys",), "city_all_" + epi_map_case_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.DEATHS_METADATA_KEY, False
    )),
    ("Covid-19 Mortality Change (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY,
         epi_map_case_layers_to_minio.DEATHS_COUNT_COL + epi_map_case_layers_to_minio.DELTA_SUFFIX,
         epi_map_case_layers_to_minio.DEATHS_COUNT_COL),
        ("Healthcare District Name", "Increase in Deaths", "Deaths"),
        ("YlOrRd",), "city_all_" + epi_map_case_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.DEATHS_METADATA_KEY, False
    )),
    ("Healthcare Facilities", (
        city_map_widget_to_minio.LayerType.POINT,
        ("NAME", "ADR",), ("Healthcare Facility Name", "Address",),
        ("red", "plus-square"), "health_care_facilities.geojson", False, False, None, False
    )),
    ("Healthcare Districts", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("CITY_HLTH_RGN_NAME",), ("Healthcare District Name",),
        ("red",), "health_districts.geojson", False, False, None, False
    )),
    # Service Delivery
    ("Open Service Requests (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY,
         service_request_map_layers_to_minio.OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.CLOSED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.NETT_OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P10_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P50_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P80_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         ),
        ("Hex ID",
         "Opened Service Requests", "Closed Service Requests", "Opened - Closed Service Requests in last month",
         "Time to close 10% of Service Requests", "Time to close 50% of Service Requests", "Time to close 80% of Service Requests",
         ),
        ("Purples",), service_request_map_layers_to_minio.HEX_COUNT_L7_FILENAME, False, False,
        None, True
    )),
    ("Service Request Durations (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY,
         service_request_map_layers_to_minio.P80_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.CLOSED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.NETT_OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P10_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P50_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         ),
        ("Hex ID", "Time to close 80% of Service Requests",
         "Opened Service Requests", "Closed Service Requests", "Opened - Closed Service Requests in last month",
         "Time to close 10% of Service Requests", "Time to close 50% of Service Requests",
         ),
        ("Purples",), service_request_map_layers_to_minio.HEX_COUNT_L7_FILENAME, False, False,
        None, True
    )),
    ("Service Request Growth (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY,
         service_request_map_layers_to_minio.OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.CLOSED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.NETT_OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P10_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P50_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P80_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         ),
        ("Hex ID", "Opened - Closed Service Requests in last month", "Opened Service Requests", "Closed Service Requests",
         "Time to close 10% of Service Requests", "Time to close 50% of Service Requests", "Time to close 80% of Service Requests",
         ),
        ("Purples",), service_request_map_layers_to_minio.HEX_COUNT_L7_FILENAME, False, False,
        None, True
    )),
    ("Open Service Requests (subcouncils)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (SC_COUNT_INDEX_PROPERTY,
         service_request_map_layers_to_minio.OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.CLOSED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.NETT_OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P10_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P50_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P80_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         ),
        ("Hex ID", "Opened - Closed Service Requests in last month", "Closed Service Requests", "Increase in Service Requests",
         "Time to close 10% of Service Requests", "Time to close 50% of Service Requests", "Time to close 80% of Service Requests",
         ),
        ("Purples",), service_request_map_layers_to_minio.SUBCOUNCIL_COUNT_FILENAME, False, False,
        None, True
    )),
    ("Service Request Durations (subcouncils)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (SC_COUNT_INDEX_PROPERTY,
         service_request_map_layers_to_minio.P80_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.CLOSED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.NETT_OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P10_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P50_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         ),
        ("Hex ID", "Time to close 80% of Service Requests",
         "Opened Service Requests", "Closed Service Requests", "Opened - Closed Service Requests in last month",
         "Time to close 10% of Service Requests", "Time to close 50% of Service Requests",
         ),
        ("Purples",), service_request_map_layers_to_minio.SUBCOUNCIL_COUNT_FILENAME, False, False,
        None, True
    )),
    ("Service Request Growth (subcouncils)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (SC_COUNT_INDEX_PROPERTY,
         service_request_map_layers_to_minio.OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.CLOSED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.NETT_OPENED_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P10_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P50_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         service_request_map_layers_to_minio.P80_DURATION_COL + SERVICE_DELIVERY_MAP_TIMEFRAME_SUFFIX,
         ),
        ("Hex ID", "Opened - Closed Service Requests in last month", "Opened Service Requests", "Closed Service Requests",
         "Time to close 10% of Service Requests", "Time to close 50% of Service Requests", "Time to close 80% of Service Requests",
         ),
        ("Purples",), service_request_map_layers_to_minio.SUBCOUNCIL_COUNT_FILENAME, False, False,
        None, True
    )),

    # Organisational Capacity
    ("Staff Capacity Count", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "absent_count", "total_assessed", "percent_absent"),
        ("Hex ID", "Staff not working", "Staff assessed", "% not working",),
        ("Oranges",), "city-absence-counts-hex.geojson", False, False,
        None, False
    )),
    ("Staff Capacity Relative", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "percent_absent", "absent_count", "total_assessed",),
        ("Hex ID", "% not working", "Staff not working", "Staff assessed", ),
        ("Oranges",), "city-absence-counts-hex.geojson", False, False,
        None, False
    )),

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
    ("Official Suburb Labels", (
        city_map_widget_to_minio.LayerType.LABEL,
        ("OFC_SBRB_NAME",), ("Official Suburb Name",),
        ("black",), "official_suburb_labels.geojson", False, False, None, False
    )),
    ("Special Rated Areas", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("SRA_NAME",), ("Name",),
        ("black",), "special_rated_areas.geojson", False, False, None, False
    )),
    ("Major Roads", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("NAME",), ("Road Type",),
        ("black",), "ct_roads.geojson", False, False, None, False
    )),
    ("Railways", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("NAME",), ("Railway Type",),
        ("black",), "ct_railways.geojson", False, False, None, False
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

CATEGORY_BUCKETS = [
    "PANDEMIC INFORMATION",
    "SERVICE DELIVERY",
    "ORGANISATIONAL CAPACITY",
    "CONTEXTUAL INFORMATION",
    "POPULATION DENSITY",
]
CATEGORY_BUCKET_MAP = {
    # Pandemic Information
    "Active Covid-19 Cases (hexes)": "PANDEMIC INFORMATION",
    "Active Covid-19 Cases (district)": "PANDEMIC INFORMATION",
    "Active Covid-19 Cases Change (district)": "PANDEMIC INFORMATION",
    "Covid-19 Mortality (hexes)": "PANDEMIC INFORMATION",
    "Covid-19 Mortality (district)": "PANDEMIC INFORMATION",
    "Covid-19 Mortality Change (district)": "PANDEMIC INFORMATION",
    "Healthcare Facilities": "PANDEMIC INFORMATION",
    "Healthcare Districts": "PANDEMIC INFORMATION",

    # Service Delivery
    "Open Service Requests (hexes)": "SERVICE DELIVERY",
    "Service Request Durations (hexes)": "SERVICE DELIVERY",
    "Service Request Growth (hexes)": "SERVICE DELIVERY",
    "Open Service Requests (subcouncils)": "SERVICE DELIVERY",
    "Service Request Durations (subcouncils)": "SERVICE DELIVERY",
    "Service Request Growth (subcouncils)": "SERVICE DELIVERY",

    # Organisational Capacity
    "Staff Capacity Counts": "Organisational Capacity",
    "Staff Capacity Relative": "Organisational Capacity",

    # Contextual Information
    "ABSD Areas": "CONTEXTUAL INFORMATION",
    "Subcouncils": "CONTEXTUAL INFORMATION",
    "Wards": "CONTEXTUAL INFORMATION",
    "Special Rated Areas": "CONTEXTUAL INFORMATION",
    "Official Suburbs": "CONTEXTUAL INFORMATION",
    "Official Suburb Labels": "CONTEXTUAL INFORMATION",
    "Major Roads": "CONTEXTUAL INFORMATION",
    "Railways": "CONTEXTUAL INFORMATION",
    "Rental Stock (flats)": "CONTEXTUAL INFORMATION",
    "Rental Stock (houses)": "CONTEXTUAL INFORMATION",
    "Rental Stock (hostels)": "CONTEXTUAL INFORMATION",
    "Areas of Informality": "CONTEXTUAL INFORMATION",

    # Population Density
    "2019 Population Estimate": "POPULATION DENSITY",
}

MARKER_ICON_PROPERTIES = {
    "CONTEXTUAL INFORMATION": {"name": "marker-cluster-context", "background_colour": "rgba(158, 158, 158, 0.6)"},
    "POPULATION DENSITY": {"name": "marker-cluster-pop-density", "background_colour": "rgba(87, 144, 193, 0.6)"},
}

BIN_QUANTILES = [0, 0, 0.5, 0.75, 1]

MAP_ZOOM = 9
DISTRICT_MAP_ZOOM = 10
MAP_RIGHT_PADDING = 200
MINIMAP_WIDTH = 150
MINIMAP_PADDING = 20
MAP_FILENAME = "service_delivery_map_widget.html"

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
    directorate_name = sys.argv[2]

    logging.info(f"Generat[ing] map widget for '{directorate_name}' directorate")

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
        map_layers_dict = {
            # layername: (location, data, layer_metadata)
            layer: (local_path, layer_gdf, layer_metadata)
            for layer, local_path, layer_gdf, layer_metadata in
            city_map_widget_to_minio.get_layers(tempdir,
                                                secrets["minio"]["edge"]["access"],
                                                secrets["minio"]["edge"]["secret"],
                                                layer_properties=SERVICE_DELIVERY_LAYER_PROPERTIES_LOOKUP,
                                                minio_path_prefix=service_request_map_layers_to_minio.SERVICE_REQUEST_MAP_PREFIX,
                                                layer_filename_prefix=directorate_file_prefix)
        }

        float_left_offset = f"{MINIMAP_WIDTH + MINIMAP_PADDING}px" if directorate_name != "*" else "0%"
        map_features = list(
            city_map_widget_to_minio.generate_map_features(map_layers_dict,
                                                           layer_properties=SERVICE_DELIVERY_LAYER_PROPERTIES_LOOKUP,
                                                           float_left_offset=float_left_offset,
                                                           choropleth_bins=BIN_QUANTILES)
        )
        logging.info("G[ot] layers")

        # logging.info("Add[ing] Marker Clusters")
        # map_features = create_marker_clusters(map_features)
        # logging.info("Add[ed] Marker Clusters")

        logging.info("Generat[ing] map")
        district_map_features = hotspot_map_widget_to_minio.generate_base_map_features(
            tempdir,
            minimap=(directorate_name != "*"), base_map_filename="absd_areas.geojson"
        )

        map_feature_generator = itertools.chain(district_map_features, map_features)

        map_zoom = DISTRICT_MAP_ZOOM if directorate_name != "*" else MAP_ZOOM
        data_map = city_map_widget_to_minio.generate_map(map_feature_generator,
                                                         map_zoom=map_zoom, map_right_padding=MAP_RIGHT_PADDING, )
        data_map = hotspot_map_widget_to_minio.add_tree_layer_control_to_map(data_map, CATEGORY_BUCKET_MAP)
        logging.info("Generat[ed] map")

        logging.info("Writ[ing] to Minio")
        city_map_widget_to_minio.write_map_to_minio(data_map, directorate_file_prefix,
                                                    tempdir,
                                                    secrets["minio"]["edge"]["access"],
                                                    secrets["minio"]["edge"]["secret"],
                                                    js_libs, css_libs, MAP_FILENAME,
                                                    map_minio_prefix=service_request_map_layers_to_minio.SERVICE_REQUEST_MAP_PREFIX)
        logging.info("Wr[ote] to Minio")
