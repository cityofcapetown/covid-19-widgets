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
import tree_layer_control

HEX_COUNT_INDEX_PROPERTY = "index"
DISTRICT_NAME_PROPERTY = "CITY_HLTH_RGN_NAME"

HOTSPOT_LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    ("Active Covid-19 Cases (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL), ("Hex ID", "Presumed Active Cases"),
        ("Reds",), city_map_layers_to_minio.HEX_L8_COUNT_SUFFIX, True, True,
        city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("Active Covid-19 Cases (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Healthcare District Name", "Presumed Active Cases"),
        ("Reds",), city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases (hexes)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL), ("Hex ID", "All Cases"),
        ("Reds",), city_map_layers_to_minio.HEX_L8_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.CUMULATIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases (district)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL),
        ("Healthcare District Name", "All Cases"),
        ("Reds",), city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
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
    # ("WCPG Testing Facilities", (
    #     city_map_widget_to_minio.LayerType.POINT,
    #     ("FACILITY_N", "STREET_ADD", "OWNERSHIP"), ("Healthcare Facility Name", "Address", "Ownership"),
    #     ("red", "stethoscope"), "wcpg_testing_facilities.geojson", False, False, None
    # )),
    ("Healthcare Districts", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("CITY_HLTH_RGN_NAME",), ("Healthcare District Name",),
        ("red",), "health_districts.geojson", False, False, None
    )),

    # Population Density
    ("2019 Population Estimate", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "PopDensity2019PerSqkm",), ("Hex ID", "People / km²",),
        ("Blues",), "sl_du_pop_est_2019_hex9.geojson", False, False, None
    )),

    # Vulnerability Indicies
    ("WCPG SEVI", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "Cluster_SE_Rounded",), ("Hex ID", "Vulnerability Score",),
        ("Oranges",), "province_sevi_hex9.geojson", False, False, None
    )),

    # Places of Risk
    ("WCED Schools", (
        city_map_widget_to_minio.LayerType.POINT,
        ("SCHL", "SUB", "QUINT"), ("School Name", "Suburb", "Quintile",),
        ("green", "book"), "wced_metro_schools_2019.geojson", False, False, None
    )),
    ("Shopping Centres (>5k m²)", (
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
        (HEX_COUNT_INDEX_PROPERTY, "gridcode",), ("Hex ID", "Activity Score",),
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
    ("SASSA Paypoint (Shops)", (
        city_map_widget_to_minio.LayerType.POINT,
        ("Store_Name", "Store_Group", "Address"), ("Store Name", "Store Group", "Address",),
        ("green", "shopping-basket"), "retail_stores.geojson", False, False, None
    )),
    # ("Employment Density", (
    #     city_map_widget_to_minio.LayerType.CHOROPLETH,
    #     ("index", "MedianEmployees", "MedianEmployeesPerFloorArea", "MaxEmployees", "SampleSize"),
    #     ("Hex ID", "Median Employees", "Median Employees per Floor Size", "Biggset Employer", "Businesses Surveyed"),
    #     ("Greens",), "employment_density_survey_hex7.geojson", False, False, None
    # )),
    ("Employment Density", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "EmploymentDensityPerSqkm",),
        ("Hex ID", "Employees / km²",),
        ("Greens",), "hh_emp_incomegrp_sp_tz2018_hex8.geojson", False, False, None
    )),

    # People at Risk
    ("Rental Stock (houses)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "Count",
         'House-Free Standing', 'House-Row House', 'House-Semi-Detached',
         'Maisonette-Row Maisonette', 'Maisonette-Semi-Detached',),
        ("Hex ID", "Number of Houses",
         'Free Standing Houses', 'Row Houses', 'Semi-Detached Houses',
         'Row Maisonettes', 'Semi-Detached Maisonettes',),
        ("Purples",), "city_house_counts.geojson", False, False, None
    )),
    ("Rental Stock (flats)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "Count", 'Flat', 'Hostel', 'Old Age Home'),
        ("Hex ID", "Total Blocks of Flats", 'Flats', 'Hostels', 'Old Age Homes'),
        ("Purples",), "city_flats_counts.geojson", False, False, None
    )),
    ("Rental Stock (hostels)", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, "Count",), ("Hex ID", "Number of Hostel Blocks",),
        ("Purples",), "city_hostel_counts.geojson", False, False, None
    )),
    ("Areas of Informality", (
        city_map_widget_to_minio.LayerType.POLYGON,
        ("AOI_NAME", "OTH_NAME", "AOI_TYPE"), ("Area Name", "Other Name", "Area Type",),
        ("purple",), "areas_of_informality_2019.geojson", False, False, None
    )),
    ("Elderly Population Density Estimate", (
        city_map_widget_to_minio.LayerType.CHOROPLETH,
        ('index', "DensityPerSqkm"), ("Hex ID", "People older than 55  / km²",),
        ("Purples",), "cpop_gt55.geojson", False, False, None
    )),
    ("Old Age Facilities", (
        city_map_widget_to_minio.LayerType.POINT,
        ('FacilityName', 'Address', 'PeopleCount', 'RoomCount', 'BedCount', 'Operator',),
        ("Name", "Physical Address", "Number of residents", "Number of Rooms", "Number of Beds", "Operator"),
        ("purple", "leaf"), "combined_senior_citizens_layer.geojson", False, False, None
    )),
    ("Adult Homeless Shelters", (
        city_map_widget_to_minio.LayerType.POINT,
        ("Name_of_Or", "Service_Ty", "Physical_a"), ("Name of Organisation", "Service Type", "Address"),
        ("purple", "bed"), "adult_homeless_shelters_coct.geojson", False, False, None
    )),
))

CATEGORY_BUCKETS = [
    "POPULATION DENSITY",
    "VULNERABILITY INDICES",
    "PLACES OF RISK",
    "PEOPLE AT RISK",
]
CATEGORY_BUCKET_MAP = {
    # Population Density
    "2019 Population Estimate": "POPULATION DENSITY",

    # "PLACES OF RISK",
    "WCED Schools": "PLACES OF RISK",
    "Shopping Centres (>5k m²)": "PLACES OF RISK",
    "Public Transport Interchanges": "PLACES OF RISK",
    "Public Transport Activity": "PLACES OF RISK",
    "Trading Locations": "PLACES OF RISK",
    "SASSA Offices": "PLACES OF RISK",
    'SASSA Paypoint (Shops)': "PLACES OF RISK",
    'Employment Density': "PLACES OF RISK",

    # "PEOPLE AT RISK",
    "Rental Stock (flats)": "PEOPLE AT RISK",
    "Rental Stock (houses)": "PEOPLE AT RISK",
    "Rental Stock (hostels)": "PEOPLE AT RISK",
    "Areas of Informality": "PEOPLE AT RISK",
    "Elderly Population Density Estimate": "PEOPLE AT RISK",
    "Old Age Facilities": "PEOPLE AT RISK",
    "Adult Homeless Shelters": "PEOPLE AT RISK",

    # "VULNERABILITY INDICES"
    "WCPG SEVI": "VULNERABILITY INDICES",
}

BIN_QUANTILES = [0, 0, 0.5, 0.75, 0.9, 0.99, 1]

MAP_ZOOM = 9
DISTRICT_MAP_ZOOM = 10
MAP_RIGHT_PADDING = 200
MINIMAP_WIDTH = 150
MINIMAP_PADDING = 20
MAP_FILENAME = "hotspot_map_widget.html"


def generate_base_map_features(tempdir, minimap=False):
    features = []

    # Health SubDistrict Outlines
    health_district_layer_path = os.path.join(tempdir, city_map_layers_to_minio.CT_HEALTH_DISTRICT_FILENAME)
    health_district_outline = folium.features.Choropleth(
        health_district_layer_path,
        name="Health Subdistricts",
        show=True,
        fill_opacity=0,
        line_color="blue"
    )
    health_district_outline.geojson.embed = False
    health_district_outline.geojson.embed_link = city_map_layers_to_minio.CT_HEALTH_DISTRICT_FILENAME
    health_district_outline.geojson.control = False

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
        ),
        health_district_outline.geojson
    ]

    # Minimap
    features += [
        folium.plugins.MiniMap(
            tile_layer=folium.TileLayer(
                tiles='https://stamen-tiles-{s}.a.ssl.fastly.net/toner-background/{z}/{x}/{y}{r}.png',
                attr='Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            ),
            width=MINIMAP_WIDTH,
            position="bottomleft",
            zoom_level_fixed=7,
        )
    ] if minimap else []

    for feature in features:
        yield feature, None


def add_tree_layer_control_to_map(map):
    base_layers = []
    overlays = []
    category_overlays = {
        bucket: [] for bucket in CATEGORY_BUCKETS
    }

    for item in map._children.values():
        if not isinstance(item, folium.map.Layer) or not item.control:
            continue

        key = item.layer_name
        item.layer_name = f" {key}"
        if not item.overlay:
            base_layers += [item]
        elif key in CATEGORY_BUCKET_MAP:
            category = CATEGORY_BUCKET_MAP[key]
            category_overlays[category] += [item]
        else:
            logging.warning(f"Putting '{key}' in the top layer - it is uncategorised!")
            overlays += [item]

    for category, category_items in category_overlays.items():
        overlays += [
            '<div class="leaflet-control-layers-separator"></div>',
            {f"<i> {category}</i>": category_items}
        ]

    tlc = tree_layer_control.TreeLayerControl(
        base_tree_entries=list(reversed(base_layers)), overlay_tree_entries=overlays,
        overlay_tree_entries_properties={
            f"<i> {bucket}</i>": {"selectAllCheckbox": True, "collapsed": True,} for bucket in CATEGORY_BUCKETS
        },
        collapsed=False, namedToggle=True,
    )

    tlc.add_to(map)

    return map


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
        extra_js_tuple = [(tree_layer_control.TreeLayerControl._js_key, tree_layer_control.TreeLayerControl._js_link),]
        extra_css_tuple = [(tree_layer_control.TreeLayerControl._css_key, tree_layer_control.TreeLayerControl._css_link),]
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
            city_map_widget_to_minio.get_layers(district_file_prefix,
                                                subdistrict_file_prefix,
                                                tempdir,
                                                secrets["minio"]["edge"]["access"],
                                                secrets["minio"]["edge"]["secret"],
                                                layer_properties=HOTSPOT_LAYER_PROPERTIES_LOOKUP, )
        }

        float_left_offset = f"{MINIMAP_WIDTH + MINIMAP_PADDING}px" if subdistrict_name != "*" else "0%"
        map_features = list(
            city_map_widget_to_minio.generate_map_features(map_layers_dict,
                                                           layer_properties=HOTSPOT_LAYER_PROPERTIES_LOOKUP,
                                                           float_left_offset=float_left_offset)
        )
        logging.info("G[ot] layers")

        logging.info("Generat[ing] map")
        district_map_features = generate_base_map_features(tempdir, minimap=(subdistrict_name != "*"))

        map_feature_generator = itertools.chain(district_map_features, map_features)

        map_zoom = DISTRICT_MAP_ZOOM if subdistrict_name != "*" else MAP_ZOOM
        data_map = city_map_widget_to_minio.generate_map(map_feature_generator,
                                                         map_zoom=map_zoom, map_right_padding=MAP_RIGHT_PADDING, )
        data_map = add_tree_layer_control_to_map(data_map)
        logging.info("Generat[ed] map")

        logging.info("Writ[ing] to Minio")
        city_map_widget_to_minio.write_map_to_minio(data_map,
                                                    district_file_prefix, subdistrict_file_prefix, tempdir,
                                                    secrets["minio"]["edge"]["access"],
                                                    secrets["minio"]["edge"]["secret"],
                                                    js_libs, css_libs, MAP_FILENAME)
        logging.info("Wr[ote] to Minio")
