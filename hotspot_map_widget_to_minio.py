import collections
import itertools
import json
import logging
import os
import sys
import tempfile

import folium.plugins

import city_map_layers_to_minio
import city_map_widget_to_minio

HEX_COUNT_INDEX_PROPERTY = "index"
DISTRICT_NAME_PROPERTY = "CITY_HLTH_RGN_NAME"

HOTSPOT_LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    ("Active Covid-19 Cases by L8 Hex", (
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL), ("Hex ID", "Presumed Active Cases"),
        "OrRd", city_map_layers_to_minio.HEX_L8_COUNT_SUFFIX, True, True, city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("Active Covid-19 Cases by District", (
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Healthcare District Name", "Presumed Active Cases"),
        "YlGn", city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases by L8 Hex", (
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL), ("Hex ID", "All Cases"),
        "OrRd", city_map_layers_to_minio.HEX_L8_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.CUMULATIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases by District", (
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL),
        ("Healthcare District Name", "All Cases"),
        "YlGn", city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.CUMULATIVE_METADATA_KEY
    )),
    ("Covid-19 Mortality by L8 Hex", (
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.DEATHS_COUNT_COL), ("Hex ID", "Deaths"),
        "OrRd", city_map_layers_to_minio.HEX_L8_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.DEATHS_METADATA_KEY
    )),
    ("Covid-19 Mortality by District", (
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.DEATHS_COUNT_COL),
        ("Healthcare District Name", "Deaths"),
        "YlGn", city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.DEATHS_METADATA_KEY
    )),
    ("Healthcare Facilities", (
        ("NAME", "ADR",), ("Healthcare Facility Name", "Address",),
        None, "health_care_facilities.geojson", False, False, None
    )),
    ("Testing Facilities", (
        ("FACILITY_N", "STREET_ADD", "OWNERSHIP"), ("Healthcare Facility Name", "Address", "Ownership"),
        None, "wcpg_testing_facilities.geojson", False, False, None
    )),

    # Population Density
    ("2019 Population Estimate", (
        ("SAL_CODE", "POP_2019",), ("SAL Code", "People",),
        "Greens", "sl_du_pop_est_2019.geojson", False, False, None
    )),

    # Vulnerability Indicies
    ("CCT Vulnerability Index", (
        ("SAL_CODE", "VLNR_IDX",), ("SAL Code", "Vulnerability Score",),
        "Reds", "cct_soc_vuln_index_targeted_adj2.geojson", False, False, None
    )),
    ("WC Vulnerability Index", (
        ("id", "Cluster_SE",), ("SAL Code", "Vulnerability Score",),
        "Reds", "provincesevi.geojson", False, False, None
    )),

    # Places of Risk
    ("WCED Schools", (
        ("SCHL", "SUB", "QUINT"), ("School Name", "Suburb", "Quintile",),
        None, "wced_metro_schools_2019.geojson", False, False, None
    )),
    ("Retail Stores", (
        ("Store_Name", "Store_Group", "Address"), ("Store Name", "Store Group", "Address",),
        None, "retail_stores.geojson", False, False, None
    )),
    ("Shopping Centres", (
        ("Centre_nam", "Physical_a",), ("Centre Name", "Address",),
        None, "shopping_centres_above_5000sqm_rode_2020.geojson", False, False, None
    )),
    ("Public Transport Interchanges", (
        ("Name", "Bus", "ParkRide", "Taxi", "Train",), ("Name", "Bus", "Park and Ride", "Taxi", "Train"),
        None, "public_transport_interchanges.geojson", False, False, None
    )),
    ("Designated Trading Location", (
        ("LOC_NAME",), ("Location Name",),
        None, "trading_location.geojson", False, False, None
    )),
    ("SASSA Local Offices", (
        ("Name", "Status"), ("Name", "Status"),
        None, "sassa_local_office_coc.geojson", False, False, None
    )),

    # People at Risk
    ("Rental Stock - Houses", (
        ("FULL_ADR", "OFC_SBRB_NAME", "SUB_TYPE"), ("Full Address", "Official Suburb", "SubType"),
        None, "cct_rental_stock_house.geojson", False, False, None
    )),
    ("Rental Stock - Flats", (
        ("BLCK_NAME", "FLAT_NAME", "TYPE",), ("Block Name", "Flat Name", "Type"),
        None, "cct_rental_stock_flats.geojson", False, False, None
    )),
    ("Areas of Informality", (
        ("AOI_NAME", "OTH_NAME", "AOI_TYPE"), ("Area Name", "Other Name", "Area Type",),
        None, "areas_of_informality_2019.geojson", False, False, None
    )),
    ("Elderly Population Density", (
        ('GRID_ID', "CNT_AGE_BIN_55PLUS"), ("Grid ID", "People older than 55 years",),
        "Greens", "sl_snth_pop_aggr_sqkm_grid.geojson", False, False, None
    )),
    ("Old Age Facilities (per Valuations Roll)", (
        ("v_ou_cd", "v_su_ext_gla_tot"), ("Valuations Use Code", "Size (sq m)",),
        None, "olderpersons_res_fac_valrole.geojson", False, False, None
    )),
    ("City Old Age Facilities", (
        ("Name_of_Or", "Physical_A", "Service_Ty"), ("Name", "Physical Address", "Service Type",),
        None, "olderpersons_res_fac_cct.geojson", False, False, None
    )),
    ("Adult Homeless Shelters", (
        ("Name_of_Or", "Service_Ty", "Physical_a"), ("Name of Organisation", "Service Type", "Address"),
        None, "adult_homeless_shelters_coct.geojson", False, False, None
    )),
))

CHOROPLETH_LAYERS = {
    *city_map_layers_to_minio.CHOROPLETH_LAYERS,
    "sl_du_pop_est_2019.geojson",
    "cct_soc_vuln_index_targeted_adj2.geojson",
    "provincesevi.geojson",
    "sl_snth_pop_aggr_sqkm_grid.geojson"
}

CATEGORY_BUCKET = {
    # Population Density
    "2019 Population Estimate": "Population Density",

    # "Places of Risk",
    "WCED Schools": "Places of Risk",
    "Retail Stores": "Places of Risk",
    "Shopping Centres": "Places of Risk",
    "Public Transport Interchanges": "Places of Risk",
    "Designated Trading Locations": "Places of Risk",
    "SASSA Local Offices": "Places of Risk",

    # "People at Risk",
    "Rental Stock - Flats": "People at Risk",
    "Rental Stock - Houses": "People at Risk",
    "Areas of Informality": "People at Risk",
    "Old Age Facilities (per Valuations Roll)": "People at Risk",
    "Old Age Facilities (City)": "People at Risk",
    "Adult Homeless Shelter": "People at Risk",

    # Vulnerability Indices
    "CCT Vulnerability Index": "Vulnerability Indices",
    "WC Vulnerability Index": "Vulnerability Indices",
}

BIN_QUANTILES = [0, 0, 0.5, 0.75, 0.9, 0.99, 1]

MAP_FILENAME = "hotspot_map_widget.html"


def generate_district_map_features():
    features = []
    # Base Layers
    features += [
        folium.TileLayer(
            name='No Base Map',
            tiles='',
            attr='No one'
        ),
        folium.TileLayer(
            name='Terrain',
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            attr='Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
        )
    ]

    # Minimap
    features += [
        folium.plugins.MiniMap(
            tile_layer=folium.TileLayer(
                tiles='https://stamen-tiles-{s}.a.ssl.fastly.net/toner-background/{z}/{x}/{y}{r}.png',
                attr='Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            ),
            zoom_level_fixed=7,
        )
    ]

    for feature in features:
        yield feature, None


def assign_features(map_features):
    features_groups_dict = {
        layer_name: [folium.features.FeatureGroup(name=layer_name, show=True), False]
        for layer_name in CATEGORY_BUCKET.values()
    }

    for feature, centroid in map_features:
        if feature.tile_name in CATEGORY_BUCKET:
            category_group = CATEGORY_BUCKET[feature.tile_name]
            feature_group, added = features_groups_dict[category_group]
            if not added:
                yield feature_group, None

                # Marking it as added
                features_groups_dict[category_group][1] = True

            sub_group = folium.plugins.FeatureGroupSubGroup(feature_group, name=feature.tile_name, show=feature.show)
            feature.add_to(sub_group)

            yield sub_group, centroid
        else:
            yield feature, centroid


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
    logging.info(f"Generat[ing] map widget for '{district_name}' district, '{subdistrict_name}' subdistrict")

    # Has to be in the outer scope as the tempdir is used in multiple places
    with tempfile.TemporaryDirectory() as tempdir:
        logging.info("Fetch[ing] Folium dependencies")
        js_libs, css_libs = city_map_widget_to_minio.pull_out_leaflet_deps(tempdir,
                                                                           secrets["proxy"]["username"],
                                                                           secrets["proxy"]["password"],
                                                                           secrets["minio"]["edge"]["access"],
                                                                           secrets["minio"]["edge"]["secret"])
        logging.info("Fetch[ed] Folium dependencies")

        logging.info("G[etting] layers")
        map_layers_dict = {
            # layername: (location, data, choropleth flag?, layer_metadata)
            layer: (local_path, layer_gdf, is_choropleth, layer_metadata)
            for layer, local_path, layer_gdf, is_choropleth, layer_metadata in
            city_map_widget_to_minio.get_layers(district_file_prefix,
                                                subdistrict_file_prefix,
                                                tempdir,
                                                secrets["minio"]["edge"]["access"],
                                                secrets["minio"]["edge"]["secret"],
                                                layer_properties=HOTSPOT_LAYER_PROPERTIES_LOOKUP,
                                                choropleth_layer_lookup=CHOROPLETH_LAYERS)
        }
        map_features = list(city_map_widget_to_minio.generate_map_features(map_layers_dict,
                                                                           layer_properties=HOTSPOT_LAYER_PROPERTIES_LOOKUP))
        logging.info("G[ot] layers")

        logging.info("Generat[ing] map")
        district_map_features = generate_district_map_features() if subdistrict_name != "*" else []

        assigned_feature_groups = assign_features(map_features)
        map_feature_generator = itertools.chain(district_map_features,
                                                assigned_feature_groups)

        data_map = city_map_widget_to_minio.generate_map(map_feature_generator)
        logging.info("Generat[ed] map")

        logging.info("Writ[ing] to Minio")
        city_map_widget_to_minio.write_map_to_minio(data_map,
                                                    district_file_prefix, subdistrict_file_prefix, tempdir,
                                                    secrets["minio"]["edge"]["access"],
                                                    secrets["minio"]["edge"]["secret"],
                                                    js_libs, css_libs, MAP_FILENAME)
        logging.info("Wr[ote] to Minio")
