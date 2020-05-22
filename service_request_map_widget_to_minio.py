import collections
import json
import logging
import os
import pprint
import sys
import tempfile
from urllib.parse import urlparse

import branca
from db_utils import minio_utils
import folium
import geopandas
import jinja2
import requests

import service_request_map_layers_to_minio

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

CITY_PROXY_DOMAIN = "internet.capetown.gov.za:8080"
DEP_DIR = "libdir"

HEX_COUNT_INDEX_PROPERTY = "index"

CITY_CENTRE = (-33.9715, 18.6021)

LAYER_PROPERTIES_TUPLES = ((
    (service_request_map_layers_to_minio.HEX_COUNT_FILENAME, (
        (HEX_COUNT_INDEX_PROPERTY, service_request_map_layers_to_minio.OPENED_COL),
        ("Hex ID", "Opened Service Requests"),
        "Reds", "Opened Service Requests", True, True
    )),
    (service_request_map_layers_to_minio.HEX_COUNT_FILENAME, (
        (HEX_COUNT_INDEX_PROPERTY, service_request_map_layers_to_minio.CLOSED_COL),
        ("Hex ID", "Closed Service Requests"),
        "Blues", "Closed Service Requests", False, True
    )),
    # (city_map_layers_to_minio.WARD_COUNT_FILENAME, (
    #     (WARD_COUNT_NAME_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL), ("Ward Name", "Case Count"),
    #     "BuPu", "Covid-19 Cases by Ward", False, True
    # )),
    ("informal_settlements.geojson", (
        ("INF_STLM_NAME",), ("Informal Settlement Name",), None, "Informal Settlements", False, False
    )),
    ("health_care_facilities.geojson", (
        ("NAME", "ADR",), ("Healthcare Facility Name", "Address",), None, "Healthcare Facilities", False, False
    )),
))
MAP_SUFFIX = "map.html"


def get_layers(directorate_file_prefix, time_period_prefix, tempdir, minio_access, minio_secret):
    for layer_filename, layer_props in LAYER_PROPERTIES_TUPLES:
        is_choropleth = layer_filename in service_request_map_layers_to_minio.CHOROPLETH_LAYERS

        # Deciding between the directorate and time period specific layer or not
        time_period_layer_filename = (
            f'{time_period_prefix}_{directorate_file_prefix}_{layer_filename}'
            if is_choropleth else
            layer_filename
        )

        local_path = os.path.join(tempdir, time_period_layer_filename)

        layer_minio_path = (
            f"{service_request_map_layers_to_minio.WIDGETS_RESTRICTED_PREFIX}"
            f"{service_request_map_layers_to_minio.SERVICE_REQUEST_MAP_PREFIX}"
            f"{time_period_layer_filename}"
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

        *_, has_metadata = layer_props
        if has_metadata:
            metadata_filename = os.path.splitext(time_period_layer_filename)[0] + ".json"
            metadata_local_path = os.path.join(tempdir, metadata_filename)
            metadata_minio_path = (
                f"{service_request_map_layers_to_minio.WIDGETS_RESTRICTED_PREFIX}"
                f"{service_request_map_layers_to_minio.SERVICE_REQUEST_MAP_PREFIX}"
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

        yield time_period_layer_filename, (local_path, layer_gdf, is_choropleth, layer_metadata, layer_props)


class FloatDiv(branca.element.MacroElement):
    """Adds a floating div in HTML canvas on top of the map."""
    _template = jinja2.Template("""
            {% macro header(this,kwargs) %}
                <style>
                    #{{this.get_name()}} {
                        position:absolute;
                        top:{{this.top}}%;
                        left:{{this.left}}%;
                        }
                </style>
            {% endmacro %}
            {% macro html(this,kwargs) %}
            <div id="{{this.get_name()}}" alt="float_div" style="z-index: 999999">
              {{this.content}}
            </div
            {% endmacro %}
            """)

    def __init__(self, content, top=10, left=0):
        super(FloatDiv, self).__init__()
        self._name = 'FloatDiv'
        self.content = content
        self.top = top
        self.left = left


def generate_map(layer_tuples):
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
    for layer_filename, (layer_path, count_gdf, is_choropleth, layer_metadata, layer_props) in layer_tuples:
        layer_lookup_fields, layer_lookup_aliases, colour_scheme, title, visible_by_default, has_metadata = layer_props
        logging.debug(f"Adding layer from path: '{layer_filename}'")
        logging.debug(f"layer_props=\n{pprint.pformat(layer_props)}")

        layer_lookup_key, *_ = layer_lookup_fields
        choropleth = folium.features.Choropleth(
            layer_path,
            data=count_gdf.reset_index(),
            name=title,
            key_on=f"feature.properties.{layer_lookup_key}",
            columns=layer_lookup_fields,
            fill_color=colour_scheme,
            highlight=True,
            show=visible_by_default,
            line_opacity=0,
            zoom_control=False
        ) if is_choropleth else folium.features.Choropleth(
            layer_path,
            name=title,
            show=visible_by_default
        )

        # Styling them there choropleths.
        # Monkey patching the choropleth GeoJSON to *not* embed
        choropleth.geojson.embed = False
        choropleth.geojson.embed_link = layer_filename

        # Adding the hover-over tooltip
        layer_tooltip = folium.features.GeoJsonTooltip(
            fields=layer_lookup_fields,
            aliases=layer_lookup_aliases
        )
        choropleth.geojson.add_child(layer_tooltip)

        # Adding missing count from metadata
        if service_request_map_layers_to_minio.METADATA_OPENED_NON_SPATIAL in layer_metadata:
            opened_non_spatial = int(layer_metadata[service_request_map_layers_to_minio.METADATA_OPENED_NON_SPATIAL])
            opened_total = int(layer_metadata[service_request_map_layers_to_minio.METADATA_OPENED_TOTAL])

            div = FloatDiv(content=f"""
                <span style="font-size: 20px; color:#FF0000"> 
                    Requests not displayed: {opened_non_spatial} ({(opened_non_spatial / opened_total):.1%}) 
                </span>
            """, top=95)

            choropleth.geojson.add_child(div)

        choropleth.add_to(m)

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
            f"{service_request_map_layers_to_minio.WIDGETS_RESTRICTED_PREFIX}"
            f"{service_request_map_layers_to_minio.SERVICE_REQUEST_MAP_PREFIX}"
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


def write_map_to_minio(map, directorate_file_prefix, time_period_prefix, map_suffix, tempdir,
                       minio_access, minio_secret, js_libs, css_libs):
    map_filename = f"{time_period_prefix}_{directorate_file_prefix}_{map_suffix}"
    local_path = os.path.join(tempdir, map_filename)

    folium.folium._default_js = js_libs
    folium.folium._default_css = css_libs

    map.save(local_path)

    result = minio_utils.file_to_minio(
        filename=local_path,
        filename_prefix_override=(
            f"{service_request_map_layers_to_minio.WIDGETS_RESTRICTED_PREFIX}"
            f"{service_request_map_layers_to_minio.SERVICE_REQUEST_MAP_PREFIX}"
        ),
        minio_bucket=MINIO_BUCKET,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=MINIO_CLASSIFICATION,
    )

    assert result


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    directorate_file_prefix = sys.argv[1]
    directorate_title = sys.argv[2]

    logging.info(f"Generat[ing] maps for '{directorate_title}'")

    # Map per time period
    map_prefixes = (
        (directorate_file_prefix, time_period_prefix)
        for time_period_prefix, _ in service_request_map_layers_to_minio.TIME_PERIODS
    )

    # Has to be in the outer scope as use the tempdir in multiple places
    with tempfile.TemporaryDirectory() as tempdir:
        logging.info("Fetch[ing] Folium dependencies")
        js_libs, css_libs = pull_out_leaflet_deps(tempdir,
                                                  secrets["proxy"]["username"], secrets["proxy"]["password"],
                                                  secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
        logging.info("Fetch[ed] Folium dependencies")

        for directorate_file_prefix, time_period_prefix in map_prefixes:
            logging.info(f"Generat[ing] map for '{directorate_title}' - '{time_period_prefix}'")

            logging.info("G[etting] layers")
            map_layers_tuples = get_layers(directorate_file_prefix, time_period_prefix,
                                           tempdir,
                                           secrets["minio"]["edge"]["access"],
                                           secrets["minio"]["edge"]["secret"])

            logging.info("G[ot] layers")

            logging.info("Generat[ing] map")
            data_map = generate_map(map_layers_tuples)
            logging.info("Generat[ed] map")

            logging.info("Writ[ing] to Minio")
            write_map_to_minio(data_map, directorate_file_prefix, time_period_prefix, MAP_SUFFIX, tempdir,
                               secrets["minio"]["edge"]["access"],
                               secrets["minio"]["edge"]["secret"],
                               js_libs, css_libs)
            logging.info("Wr[ote] to Minio")
