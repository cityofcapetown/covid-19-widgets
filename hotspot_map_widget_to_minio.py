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
    ("Active Covid-19 Cases (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL), ("Hex ID", "Presumed Active Cases"),
        ("Greys",), city_map_layers_to_minio.HEX_L8_COUNT_SUFFIX, True, True,
        city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("Active Covid-19 Cases (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Healthcare District Name", "Presumed Active Cases"),
        ("Greys",), city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL), ("Hex ID", "All Cases"),
        ("Greys",), city_map_layers_to_minio.HEX_L8_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.CUMULATIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL),
        ("Healthcare District Name", "All Cases"),
        ("Greys",), city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.CUMULATIVE_METADATA_KEY
    )),
    ("Covid-19 Mortality (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.DEATHS_COUNT_COL), ("Hex ID", "Deaths"),
        ("Greys",), city_map_layers_to_minio.HEX_L8_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.DEATHS_METADATA_KEY
    )),
    ("Covid-19 Mortality (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.DEATHS_COUNT_COL),
        ("Healthcare District Name", "Deaths"),
        ("Greys",), city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.DEATHS_METADATA_KEY
    )),
    ("Healthcare Facilities", (
        city_map_widget_to_minio.LayerType.POINT,
        ("NAME", "ADR",), ("Healthcare Facility Name", "Address",),
        ("red", "plus-square"), "health_care_facilities.geojson", False, False, None
    )),
    ("WCPG Testing Facilities", (
        city_map_widget_to_minio.LayerType.POINT,
        ("FACILITY_N", "STREET_ADD", "OWNERSHIP"), ("Healthcare Facility Name", "Address", "Ownership"),
        ("red", "stethoscope"), "wcpg_testing_facilities.geojson", False, False, None
    )),

    # Population Density
    ("2019 Population Estimate", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        ("index", "PopDensity2019",), ("Hex ID", "People / sq.m",),
        ("Blues",), "sl_du_pop_est_2019_hex9.geojson", False, False, None
    )),

    # Vulnerability Indicies
    ("WCPG SEVI", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        ("index", "Cluster_SE",), ("Hex ID", "Vulnerability Score",),
        ("Reds",), "province_sevi_hex9.geojson", False, False, None
    )),

    # Places of Risk
    ("WCED Schools", (
        city_map_widget_to_minio.LayerType.POINT,
        ("SCHL", "SUB", "QUINT"), ("School Name", "Suburb", "Quintile",),
        ("green", "book"), "wced_metro_schools_2019.geojson", False, False, None
    )),
    ("Retail Stores", (
        city_map_widget_to_minio.LayerType.POINT,
        ("Store_Name", "Store_Group", "Address"), ("Store Name", "Store Group", "Address",),
        ("green", "shopping-basket"), "retail_stores.geojson", False, False, None
    )),
    ("Shopping Centres (>5k sq.m)", (
        city_map_widget_to_minio.LayerType.POINT,
        ("Centre_nam", "Physical_a",), ("Centre Name", "Address",),
        ("green", "shopping-cart"), "shopping_centres_above_5000sqm_rode_2020.geojson", False, False, None
    )),
    ("Public Transport Interchanges", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("Name", "Bus", "ParkRide", "Taxi", "Train",), ("Name", "Bus", "Park and Ride", "Taxi", "Train"),
        ("green",), "public_transport_interchanges.geojson", False, False, None
    )),
    ("Public Transport Activity", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        ("index", "gridcode",), ("Hex ID", "Activity Score",),
        ("Greens",), "public_transport_activity_levels_hex9.geojson", False, False, None
    )),
    ("Trading Locations", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("LOC_NAME",), ("Location Name",),
        ("green",), "trading_location.geojson", False, False, None
    )),
    ("SASSA Offices", (
        city_map_widget_to_minio.LayerType.POINT,
        ("Name", "Status"), ("Name", "Status"),
        ("green", "building"), "sassa_local_office_coc.geojson", False, False, None
    )),
    ("Employment Density", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        ("index", "MedianEmployees", "MedianEmployeesPerFloorArea", "MaxEmployees", "SampleSize"),
        ("Hex ID", "Median Employees", "Median Employees per Floor Size", "Biggset Employer", "Businesses Surveyed"),
        ("Greens",), "employment_density_survey_hex7.geojson", False, False, None
    )),

    # People at Risk
    ("Rental Stock (houses)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        ("index", "Count",
         'House-Free Standing', 'House-Row House', 'House-Semi-Detached',
         'Maisonette-Row Maisonette', 'Maisonette-Semi-Detached',),
        ("Hex ID", "Number of Houses",
         'Free Standing Houses', 'Row Houses', 'Semi-Detached Houses',
         'Row Maisonettes', 'Semi-Detached Maisonettes',),
        ("Purples",), "city_house_counts.geojson", False, False, None
    )),
    ("Rental Stock (flats)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        ("index", "Count", 'Flat', 'Hostel', 'Old Age Home'),
        ("Hex ID", "Total Blocks of Flats", 'Flats', 'Hostels', 'Old Age Homes'),
        ("Purples",), "city_flats_counts.geojson", False, False, None
    )),
    ("Rental Stock (hostels)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        ("index", "Count",), ("Hex ID", "Number of Hostel Blocks",),
        ("Purples",), "city_hostel_counts.geojson", False, False, None
    )),
    ("Areas of Informality", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("AOI_NAME", "OTH_NAME", "AOI_TYPE"), ("Area Name", "Other Name", "Area Type",),
        ("purple",), "areas_of_informality_2019.geojson", False, False, None
    )),
    # ("Elderly Population Density", (
    #     city_map_widget_to_minio.LayerType.CHOROPLETH,
    #     ('GRID_ID', "CNT_AGE_BIN_55PLUS"), ("Grid ID", "People older than 55 years",),
    #     ("Purples",), "sl_snth_pop_aggr_sqkm_grid.geojson", False, False, None
    # )),
    # ("Elderly Population Density", (
    #     city_map_widget_to_minio.LayerType.CHOROPLETH,
    #     ('index', "gridcode"), ("Hex ID", "Older Population Score",),
    #     ("Purples",), "hdx_pop_estimates_elderly_hex9.geojson", False, False, None
    # )),
    ("Old Age Facilities (by use)", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("v_ou_cd", "v_su_ext_gla_tot"), ("Valuations Use Code", "Size (sq m)",),
        ("purple",), "olderpersons_res_fac_valrole.geojson", False, False, None
    )),
    ("City Old Age Facilities", (
        city_map_widget_to_minio.LayerType.POINT,
        ("Name_of_Or", "Physical_A", "Service_Ty"), ("Name", "Physical Address", "Service Type",),
        ("purple", "leaf"), "olderpersons_res_fac_cct.geojson", False, False, None
    )),
    ("Adult Homeless Shelters", (
        city_map_widget_to_minio.LayerType.POINT,
        ("Name_of_Or", "Service_Ty", "Physical_a"), ("Name of Organisation", "Service Type", "Address"),
        ("purple", "bed"), "adult_homeless_shelters_coct.geojson", False, False, None
    )),
))

CATEGORY_BUCKET = {
    # Population Density
    "<small>2019 Population Estimate</small>": "POPULATION DENSITY",

    # "PLACES OF RISK",
    "<small>WCED Schools</small>": "PLACES OF RISK",
    "<small>Retail Stores</small>": "PLACES OF RISK",
    "<small>Shopping Centres (>5k sq.m)</small>": "PLACES OF RISK",
    "<small>Public Transport Interchanges</small>": "PLACES OF RISK",
    "<small>Public Transport Activity</small>": "PLACES OF RISK",
    "<small>Trading Locations</small>": "PLACES OF RISK",
    "<small>SASSA Offices</small>": "PLACES OF RISK",

    # "PEOPLE AT RISK",
    "<small>Rental Stock (flats)</small>": "PEOPLE AT RISK",
    "<small>Rental Stock (houses)</small>": "PEOPLE AT RISK",
    "<small>Rental Stock (hostels)</small>": "PEOPLE AT RISK",
    "<small>Areas of Informality</small>": "PEOPLE AT RISK",
    #"Elderly Population Density": "PEOPLE AT RISK",
    "<small>Old Age Facilities (by use)</small>": "PEOPLE AT RISK",
    "<small>City Old Age Facilities</small>": "PEOPLE AT RISK",
    "<small>Adult Homeless Shelter</small>": "PEOPLE AT RISK",

    # "VULNERABILITY INDICES"
    "<small>WCPG SEVI</small>": "VULNERABILITY INDICES",
}

BIN_QUANTILES = [0, 0, 0.5, 0.75, 0.9, 0.99, 1]

MAP_ZOOM = 9
MAP_RIGHT_PADDING = 200
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
            # layername: (location, data, layer_metadata)
            layer: (local_path, layer_gdf, layer_metadata)
            for layer, local_path, layer_gdf, layer_metadata in
            city_map_widget_to_minio.get_layers(district_file_prefix,
                                                subdistrict_file_prefix,
                                                tempdir,
                                                secrets["minio"]["edge"]["access"],
                                                secrets["minio"]["edge"]["secret"],
                                                layer_properties=HOTSPOT_LAYER_PROPERTIES_LOOKUP)
        }
        map_features = list(city_map_widget_to_minio.generate_map_features(map_layers_dict,
                                                                           layer_properties=HOTSPOT_LAYER_PROPERTIES_LOOKUP))
        logging.info("G[ot] layers")

        logging.info("Generat[ing] map")
        district_map_features = generate_district_map_features() if subdistrict_name != "*" else []

        assigned_feature_groups = assign_features(map_features)
        map_feature_generator = itertools.chain(district_map_features,
                                                assigned_feature_groups)

        data_map = city_map_widget_to_minio.generate_map(map_feature_generator,
                                                         map_zoom=MAP_ZOOM, map_right_padding=MAP_RIGHT_PADDING)
        logging.info("Generat[ed] map")

        logging.info("Writ[ing] to Minio")
        city_map_widget_to_minio.write_map_to_minio(data_map,
                                                    district_file_prefix, subdistrict_file_prefix, tempdir,
                                                    secrets["minio"]["edge"]["access"],
                                                    secrets["minio"]["edge"]["secret"],
                                                    js_libs, css_libs, MAP_FILENAME)
        logging.info("Wr[ote] to Minio")
