import collections
import json
import logging
import os
import sys
import tempfile
from enum import Enum
from urllib.parse import urlparse

from db_utils import minio_utils
import folium
import geopandas
import requests

import city_map_layers_to_minio
import float_div
import geojson_markers

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

CITY_PROXY_DOMAIN = "internet.capetown.gov.za:8080"
DEP_DIR = "libdir"

WARD_COUNT_NAME_PROPERTY = "WardNo"
HEX_COUNT_INDEX_PROPERTY = "index"
DISTRICT_NAME_PROPERTY = "CITY_HLTH_RGN_NAME"

CITY_CENTRE = (-33.9715, 18.6021)


class LayerType(Enum):
    CHOROPLETH = "choropleth"
    POINT = "point"
    POLYGON = "polygon"


LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    ("Active Covid-19 Cases by L7 Hex", (
        LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL), ("Hex ID", "Presumed Active Cases"),
        "OrRd", city_map_layers_to_minio.HEX_L7_COUNT_SUFFIX, True, True, city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("Active Covid-19 Cases by Ward", (
        LayerType.CHOROPLETH,
        (WARD_COUNT_NAME_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Ward Name", "Presumed Active Cases"),
        "BuPu", city_map_layers_to_minio.WARD_COUNT_SUFFIX, False, True, city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("Active Covid-19 Cases by District", (
        LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Healthcare District Name", "Presumed Active Cases"),
        "YlGn", city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.ACTIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases by L7 Hex", (
        LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL), ("Hex ID", "All Cases"),
        "OrRd", city_map_layers_to_minio.HEX_L7_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.CUMULATIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases by Ward", (
        LayerType.CHOROPLETH,
        (WARD_COUNT_NAME_PROPERTY, city_map_layers_to_minio.CASE_COUNT_COL), ("Ward Name", "All Cases"),
        "BuPu", city_map_layers_to_minio.WARD_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.CUMULATIVE_METADATA_KEY
    )),
    ("All Covid-19 Cases by District", (
        LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, city_map_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Healthcare District Name", "Presumed Active Cases"),
        "YlGn", city_map_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        city_map_layers_to_minio.CUMULATIVE_METADATA_KEY
    )),
    ("Informal Settlements", (
        LayerType.POLYGON,
        ("INF_STLM_NAME",), ("Informal Settlement Name",),
        None, "informal_settlements.geojson", False, False, None
    )),
    ("Healthcare Facilities", (
        LayerType.POINT,
        ("NAME", "ADR",), ("Healthcare Facility Name", "Address",),
        None, "health_care_facilities.geojson", False, False, None
    )),
    ("Healthcare Districts", (
        LayerType.POLYGON,
        ("CITY_HLTH_RGN_NAME", ), ("Healthcare District Name",),
        None, "health_districts.geojson", False, False, None
    )),
))

BIN_QUANTILES = [0, 0, 0.5, 0.75, 0.9, 0.99, 1]

MAP_FILENAME = "map_widget.html"


def get_layers(district_file_prefix, subdistrict_file_prefix, tempdir, minio_access, minio_secret,
               layer_properties=LAYER_PROPERTIES_LOOKUP):
    for layer, layer_properties in layer_properties.items():
        layer_type, *_, layer_suffix, _1, has_metadata, _3 = layer_properties

        layer_filename = (f"{district_file_prefix}_{subdistrict_file_prefix}_{layer_suffix}"
                          if layer_type is LayerType.CHOROPLETH and has_metadata
                          else layer_suffix)

        local_path = os.path.join(tempdir, layer_filename)

        layer_minio_path = (
            f"{city_map_layers_to_minio.CASE_MAP_PREFIX}"
            f"{layer_filename}"
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

        # Getting the layer's metadata
        if has_metadata:
            metadata_filename = os.path.splitext(layer_filename)[0] + ".json"
            metadata_local_path = os.path.join(tempdir, metadata_filename)
            metadata_minio_path = (
                f"{city_map_layers_to_minio.CASE_MAP_PREFIX}"
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


def _get_choropleth_bins(count_series):
    bins = [0]
    data_edges = list(count_series.quantile(BIN_QUANTILES).values)

    bins += [
        val for val in data_edges if val > bins[-1]
    ]

    # Adding extra padding if we have a bi-modal plot (colorbrewer needs 3 bins, at least)
    if len(bins) <= 3:
        bins.insert(0, 0)

    return bins


def generate_map_features(layers_dict, layer_properties=LAYER_PROPERTIES_LOOKUP):
    # Going layer by layer
    for title, (layer_path, count_gdf, layer_metadata) in layers_dict.items():
        (layer_type,
         layer_lookup_fields, layer_lookup_aliases,
         colour_scheme, layer_suffix, visible_by_default,
         has_metadata, metadata_key) = layer_properties[title]

        # Everything gets packed into a feature group
        layer_feature_group = folium.features.FeatureGroup(
            name=title,
            show=visible_by_default
        )
        *_, layer_filename = os.path.split(layer_path)

        if layer_type in {LayerType.CHOROPLETH, LayerType.POLYGON}:
            layer_lookup_key, choropleth_key = layer_lookup_fields if layer_type is LayerType.CHOROPLETH else (None, None,)

            choropleth = folium.features.Choropleth(
                layer_path,
                data=count_gdf.reset_index(),
                name=title,
                key_on=f"feature.properties.{layer_lookup_key}",
                columns=[layer_lookup_key, choropleth_key],
                fill_color=colour_scheme,
                highlight=True,
                show=visible_by_default,
                line_opacity=0,
                bins=_get_choropleth_bins(count_gdf[choropleth_key]),
                nan_fill_opacity=0,
            ) if layer_type is LayerType.CHOROPLETH else folium.features.Choropleth(
                layer_path,
                name=title,
                show=visible_by_default
            )

            # Monkey patching the choropleth GeoJSON to *not* embed
            choropleth.geojson.embed = False
            choropleth.geojson.embed_link = f"{layer_filename}"

            # Adding the hover-over tooltip
            layer_tooltip = folium.features.GeoJsonTooltip(
                fields=layer_lookup_fields,
                aliases=layer_lookup_aliases
            )
            choropleth.geojson.add_child(layer_tooltip)

            layer_feature_group.add_child(choropleth.geojson)
        elif layer_type is LayerType.POINT:
            tooltip_item_array = "['" + "'],['".join([
                "','".join((col, alias,))
                for col, alias in zip(layer_lookup_fields, layer_lookup_aliases)
            ]) + "']"

            tooltip_callback = f"""
                function (feature) {{
                    let handleObject = (feature)=>typeof(feature)=='object' ? JSON.stringify(feature) : feature;
                    return '<table>' +
                        String(
                            [{tooltip_item_array}].map(
                                columnTuple=>
                                    `<tr style="text-align: left;">
                                    <th style="padding: 4px; padding-right: 10px;">
                                        ${{ handleObject(columnTuple[1]).toLocaleString() }}
                                    </th>
                                    <td style="padding: 4px;">
                                        ${{ handleObject(feature.properties[columnTuple[0]]).toLocaleString() }}
                                    </td></tr>`
                            ).join('')
                        )
                        + '</table>'
                }};
            """

            markers = geojson_markers.GeoJsonMarkers(
                count_gdf.reset_index(), embed=True,
                tooltip_callback=tooltip_callback,
                name=title, show=visible_by_default
            )
            markers.embed = False
            markers.embed_link = layer_filename

            layer_feature_group.add_child(markers)

        # If this is a visible layer, calculating the centroids
        centroids = list(count_gdf.geometry.map(
           lambda shape: (shape.centroid.y.round(4), shape.centroid.x.round(4))
        )) if visible_by_default else []

        # Adding missing count from metadata
        if metadata_key in layer_metadata:
            cases_not_displayed = layer_metadata[metadata_key][city_map_layers_to_minio.NOT_SPATIAL_CASE_COUNT]
            total_count = layer_metadata[metadata_key][city_map_layers_to_minio.CASE_COUNT_TOTAL]

            div = float_div.FloatDiv(content=(
                "<span style='font-size: 20px; color:#FF0000'>"
                f"Cases not displayed: {cases_not_displayed} ({cases_not_displayed / total_count:.1%} of total)"
                "</span>"
            ), top=95)
            layer_feature_group.add_child(div)

        yield layer_feature_group, centroids


def generate_map(map_features):
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

    # Adding the features
    map_centroids = []
    for feature, centroids in map_features:
        m.add_child(feature)
        map_centroids += centroids if centroids else []

    # Setting the map zoom using any visible layers
    m.fit_bounds(map_centroids, padding_bottom_right=(0, 100))

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
            f"{city_map_layers_to_minio.CASE_MAP_PREFIX}"
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


def write_map_to_minio(city_map, district_file_prefix, subdistrict_file_prefix, tempdir, minio_access, minio_secret,
                       js_libs, css_libs, map_suffix=MAP_FILENAME):
    map_filename = f"{district_file_prefix}_{subdistrict_file_prefix}_{map_suffix}"
    local_path = os.path.join(tempdir, map_filename)

    folium.folium._default_js = js_libs
    folium.folium._default_css = css_libs

    city_map.save(local_path)

    result = minio_utils.file_to_minio(
        filename=local_path,
        filename_prefix_override=(
            f"{city_map_layers_to_minio.CASE_MAP_PREFIX}"
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

    district_file_prefix = sys.argv[1]
    district_name = sys.argv[2]

    subdistrict_file_prefix = sys.argv[3]
    subdistrict_name = sys.argv[4]
    logging.info(f"Generat[ing] map widget for '{district_name}' district, '{subdistrict_name}' subdistrict")

    # Has to be in the outer scope as the tempdir is used in multiple places
    with tempfile.TemporaryDirectory() as tempdir:
        logging.info("Fetch[ing] Folium dependencies")
        js_libs, css_libs = pull_out_leaflet_deps(tempdir,
                                                  secrets["proxy"]["username"], secrets["proxy"]["password"],
                                                  secrets["minio"]["edge"]["access"],
                                                  secrets["minio"]["edge"]["secret"])
        logging.info("Fetch[ed] Folium dependencies")

        logging.info("G[etting] layers")
        map_layers_dict = {
            # layername: (location, data, choropleth flag?, layer_metadata)
            layer: (local_path, layer_gdf, layer_metadata)
            for layer, local_path, layer_gdf, layer_metadata in get_layers(district_file_prefix,
                                                                           subdistrict_file_prefix,
                                                                           tempdir,
                                                                           secrets["minio"]["edge"]["access"],
                                                                           secrets["minio"]["edge"]["secret"])
        }
        logging.info("G[ot] layers")

        logging.info("Generat[ing] map")
        map_feature_generator = generate_map_features(map_layers_dict)
        data_map = generate_map(map_feature_generator)
        logging.info("Generat[ed] map")

        logging.info("Writ[ing] to Minio")
        write_map_to_minio(data_map,
                           district_file_prefix, subdistrict_file_prefix, tempdir,
                           secrets["minio"]["edge"]["access"],
                           secrets["minio"]["edge"]["secret"],
                           js_libs, css_libs)
        logging.info("Wr[ote] to Minio")
