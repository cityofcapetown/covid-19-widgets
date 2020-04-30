import collections
import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import folium
import geopandas

import city_map_layers_to_minio

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

WIDGETS_RESTRICTED_PREFIX = "widgets/private/city_map_"

CITY_CASE_DATA_FILENAME = "ct_all_cases.csv"
WARD_COUNT_FILENAME = "ward_case_count.geojson"
HEX_COUNT_FILENAME = "hex_case_count.geojson"

WARD_COUNT_NAME_PROPERTY = "WardNo"
HEX_COUNT_INDEX_PROPERTY = "index"

CASE_COUNT_COL_OLD = "date_of_diagnosis1"
CASE_COUNT_COL = "CaseCount"
CITY_CENTRE = (-33.9715, 18.6021)

LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    (city_map_layers_to_minio.WARD_COUNT_FILENAME, (
        (WARD_COUNT_NAME_PROPERTY, CASE_COUNT_COL), ("Ward Name", "Case Count"), "BuPu", "Covid-19 Cases by Ward", True
    )),
    (city_map_layers_to_minio.HEX_COUNT_FILENAME, (
        (HEX_COUNT_INDEX_PROPERTY, CASE_COUNT_COL), ("Hex ID", "Case Count"), "OrRd", "Covid-19 Cases by L7 Hex", False
    )),
    ("ct_wards.geojson", (
        (WARD_COUNT_NAME_PROPERTY,), ("Ward Name",), "BuPu", "City of Cape Town Wards", False
    )),
    ("cct_hex_polygons_7.geojson", (
        (HEX_COUNT_INDEX_PROPERTY,), ("Hex ID",), "OrRd", "City of Cape Town L7 Hexes", False
    )),
    ("informal_settlements.geojson", (
        ("INF_STLM_NAME",), ("Informal Settlement Name",), None, "Informal Settlements", False
    )),
    ("health_care_facilities.geojson", (
        ("NAME", "ADR",), ("Healthcare Facility Name", "Address",), None, "Healthcare Facilities", False
    )),
))
MAP_FILENAME = "widget.html"


def get_layers(tempdir, minio_access, minio_secret):
    for layer in LAYER_PROPERTIES_LOOKUP.keys():
        local_path = os.path.join(tempdir, layer)

        minio_utils.minio_to_file(
            filename=local_path,
            minio_filename_override=WIDGETS_RESTRICTED_PREFIX + layer,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        layer_gdf = geopandas.read_file(local_path)

        yield layer, local_path, layer_gdf


def generate_map(layers_dict):
    m = folium.Map(
        location=CITY_CENTRE, zoom_start=9,
        tiles="",
        prefer_canvas=True
    )

    # Feature Map
    m.add_child(
        folium.TileLayer(
            name='Base Map',
            tiles='https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
            attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; '
                 '<a href="https://carto.com/attributions">CARTO</a>'
        )
    )

    # Going layer by layer
    for layer_filename, (layer_path, count_gdf, is_choropleth) in layers_dict.items():
        (layer_lookup_fields, layer_lookup_aliases,
         colour_scheme, title, visible_by_default) = LAYER_PROPERTIES_LOOKUP[layer_filename]

        layer_lookup_key, *_ = layer_lookup_fields
        choropleth = folium.features.Choropleth(
            layer_path,
            data=count_gdf.reset_index(),
            name=title,
            key_on=f"feature.properties.{layer_lookup_key}",
            columns=[layer_lookup_key, CASE_COUNT_COL],
            fill_color=colour_scheme,
            highlight=True,
            show=visible_by_default
        ) if is_choropleth else folium.features.Choropleth(
            layer_path,
            name=title,
            show=visible_by_default
        )

        # Styling them there choropleths.
        # Monkey patching the choropleth GeoJSON to *not* embed
        choropleth.geojson.embed = False
        choropleth.geojson.embed_link = "city_" + layer_filename

        # Adding the hover-over tooltip
        layer_tooltip = folium.features.GeoJsonTooltip(
            fields=layer_lookup_fields,
            aliases=layer_lookup_aliases
        )
        choropleth.geojson.add_child(layer_tooltip)

        choropleth.add_to(m)

    # Layer Control
    layer_control = folium.LayerControl()
    layer_control.add_to(m)

    return m


def write_map_to_minio(city_map, tempdir, minio_access, minio_secret):
    local_path = os.path.join(tempdir, MAP_FILENAME)
    city_map.save(local_path)

    result = minio_utils.file_to_minio(
        filename=local_path,
        filename_prefix_override=WIDGETS_RESTRICTED_PREFIX,
        minio_bucket=MINIO_BUCKET,
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

    # Has to be in the outer scope as use the tempdir in multiple places
    with tempfile.TemporaryDirectory() as tempdir:
        logging.info("G[etting] layers")
        map_layers_dict = {
            # layername: (location, data, choropleth flag?)
            layer: (local_path, layer_gdf, layer in city_map_layers_to_minio.CHOROPLETH_LAYERS)
            for layer, local_path, layer_gdf in get_layers(tempdir,
                                                           secrets["minio"]["edge"]["access"],
                                                           secrets["minio"]["edge"]["secret"])
        }
        logging.info("G[ot] layers")

        logging.info("Generat[ing] map")
        data_map = generate_map(map_layers_dict)
        logging.info("Generat[ed] map")

        logging.info("Writ[ing] to Minio")
        write_map_to_minio(data_map, tempdir,
                           secrets["minio"]["edge"]["access"],
                           secrets["minio"]["edge"]["secret"])
        logging.info("Wr[ote] to Minio")
