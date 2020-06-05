import collections
import json
import logging
import os
import sys
import tempfile
from urllib.parse import urlparse

from db_utils import minio_utils
import folium
import geopandas
import jinja2
import requests

import city_map_layers_to_minio
import float_div

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

CITY_PROXY_DOMAIN = "internet.capetown.gov.za:8080"
DEP_DIR = "libdir"

WARD_COUNT_NAME_PROPERTY = "WardNo"
HEX_COUNT_INDEX_PROPERTY = "index"

CITY_CENTRE = (-33.9715, 18.6021)

LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    (city_map_layers_to_minio.HEX_COUNT_FILENAME, (
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL), ("Hex ID", "Case Count"),
        "OrRd", "Covid-19 Cases by L7 Hex", True, True
    )),
    (city_map_layers_to_minio.WARD_COUNT_FILENAME, (
        (WARD_COUNT_NAME_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL), ("Ward Name", "Case Count"),
        "BuPu", "Covid-19 Cases by Ward", False, True
    )),
    ("informal_settlements.geojson", (
        ("INF_STLM_NAME",), ("Informal Settlement Name",), None, "Informal Settlements", False, False
    )),
    ("health_care_facilities.geojson", (
        ("NAME", "ADR",), ("Healthcare Facility Name", "Address",), None, "Healthcare Facilities", False, False
    )),
    ("health_districts.geojson", (
        ("CITY_HLTH_RGN_NAME", ), ("Healthcare District Name", ), None, "Healthcare Districts", False, False
    )),
))
MAP_FILENAME = "widget.html"


def get_layers(tempdir, minio_access, minio_secret):
    for layer in LAYER_PROPERTIES_LOOKUP.keys():
        local_path = os.path.join(tempdir, layer)

        layer_minio_path = (
            f"{city_map_layers_to_minio.WIDGETS_RESTRICTED_PREFIX}{city_map_layers_to_minio.CITY_MAP_PREFIX}"
            f"{layer}"
        )
        minio_utils.minio_to_file(
            filename=local_path,
            minio_filename_override=layer_minio_path,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        layer_gdf = geopandas.read_file(local_path)

        *_, has_metadata = LAYER_PROPERTIES_LOOKUP[layer]
        if has_metadata:
            metadata_filename = os.path.splitext(layer)[0] + ".json"
            metadata_local_path = os.path.join(tempdir, metadata_filename)
            metadata_minio_path = (
                f"{city_map_layers_to_minio.WIDGETS_RESTRICTED_PREFIX}{city_map_layers_to_minio.CITY_MAP_PREFIX}"
                f"{metadata_filename}"
            )

            minio_utils.minio_to_file(
                filename=metadata_local_path,
                minio_filename_override=metadata_minio_path,
                minio_bucket=MINIO_BUCKET,
                minio_key=minio_access,
                minio_secret=minio_secret,
                data_classification=MINIO_CLASSIFICATION,
            )
            with open(metadata_local_path, "r") as metadata_file:
                layer_metadata = json.load(metadata_file)
        else:
            layer_metadata = {}

        yield layer, local_path, layer_gdf, layer_metadata


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
    for layer_filename, (layer_path, count_gdf, is_choropleth, layer_metadata) in layers_dict.items():
        (layer_lookup_fields, layer_lookup_aliases,
         colour_scheme, title, visible_by_default, has_metadata) = LAYER_PROPERTIES_LOOKUP[layer_filename]

        layer_lookup_key, *_ = layer_lookup_fields
        choropleth = folium.features.Choropleth(
            layer_path,
            data=count_gdf.reset_index(),
            name=title,
            key_on=f"feature.properties.{layer_lookup_key}",
            columns=[layer_lookup_key, city_map_layers_to_minio.CASE_COUNT_COL],
            fill_color=colour_scheme,
            highlight=True,
            show=visible_by_default,
            line_opacity=0
        ) if is_choropleth else folium.features.Choropleth(
            layer_path,
            name=title,
            show=visible_by_default
        )

        # Monkey patching the choropleth GeoJSON to *not* embed
        choropleth.geojson.embed = False
        choropleth.geojson.embed_link = (
            f"{city_map_layers_to_minio.CITY_MAP_PREFIX}{layer_filename}"
        )

        # Adding the hover-over tooltip
        layer_tooltip = folium.features.GeoJsonTooltip(
            fields=layer_lookup_fields,
            aliases=layer_lookup_aliases
        )
        choropleth.geojson.add_child(layer_tooltip)

        # Rather repacking things into a feature group
        choropleth_feature_group = folium.features.FeatureGroup(
            name=title,
            show=visible_by_default
        )
        choropleth_feature_group.add_child(choropleth.geojson)

        # Adding missing count from metadata
        if city_map_layers_to_minio.NOT_SPATIAL_CASE_COUNT in layer_metadata:
            cases_not_displayed = layer_metadata[city_map_layers_to_minio.NOT_SPATIAL_CASE_COUNT]
            div = float_div.FloatDiv(content=(
                "<span style='font-size: 20px; color:#FF0000'>" 
                    f"Cases not displayed: {cases_not_displayed}"
                "</span>"
            ), top=95)
            choropleth_feature_group.add_child(div)

        choropleth_feature_group.add_to(m)

    # Layer Control
    layer_control = folium.LayerControl(collapsed=False)
    layer_control.add_to(m)

    return m


def get_leaflet_dep_file(url, tempdir, http_session, minio_access, minio_secret):
    filename = os.path.basename(
        urlparse(url).path
    )

    local_path = os.path.join(tempdir, filename)
    resp = http_session.get(url)

    with open(local_path, "wb") as dep_file:
        dep_file.write(resp.content)

    minio_utils.file_to_minio(
        filename=local_path,
        filename_prefix_override=(
            f"{city_map_layers_to_minio.WIDGETS_RESTRICTED_PREFIX}"
            f"{DEP_DIR}/"
        ),
        minio_bucket=MINIO_BUCKET,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=MINIO_CLASSIFICATION,
    )

    new_path = (
        f"{DEP_DIR}/{filename}"
    )

    return new_path


def pull_out_leaflet_deps(tempdir, proxy_username, proxy_password, minio_access, minio_secret):
    http_session = requests.Session()

    proxy_string = f'http://{proxy_username}:{proxy_password}@{CITY_PROXY_DOMAIN}/'
    http_session.proxies = {
        "http": proxy_string,
        "https": proxy_string
    }

    js_libs = [
        (key, get_leaflet_dep_file(url, tempdir, http_session, minio_access, minio_secret))
        for key, url in folium.folium._default_js
    ]

    css_libs = [
        (key, get_leaflet_dep_file(url, tempdir, http_session, minio_access, minio_secret))
        for key, url in folium.folium._default_css
    ]

    return js_libs, css_libs


def write_map_to_minio(city_map, tempdir, minio_access, minio_secret, js_libs, css_libs):
    local_path = os.path.join(tempdir, MAP_FILENAME)

    folium.folium._default_js = js_libs
    folium.folium._default_css = css_libs

    city_map.save(local_path)

    result = minio_utils.file_to_minio(
        filename=local_path,
        filename_prefix_override=(
            f"{city_map_layers_to_minio.WIDGETS_RESTRICTED_PREFIX}"
            f"{city_map_layers_to_minio.CITY_MAP_PREFIX}"
        ),
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
        logging.info("Fetch[ing] Folium dependencies")
        js_libs, css_libs = pull_out_leaflet_deps(tempdir,
                                                  secrets["proxy"]["username"], secrets["proxy"]["password"],
                                                  secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
        logging.info("Fetch[ed] Folium dependencies")

        logging.info("G[etting] layers")
        map_layers_dict = {
            # layername: (location, data, choropleth flag?)
            layer: (local_path, layer_gdf, layer in city_map_layers_to_minio.CHOROPLETH_LAYERS, layer_metadata)
            for layer, local_path, layer_gdf, layer_metadata in get_layers(tempdir,
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
                           secrets["minio"]["edge"]["secret"],
                           js_libs, css_libs)
        logging.info("Wr[ote] to Minio")
