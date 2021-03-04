import collections
import functools
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
import numpy
import requests

import epi_map_case_layers_to_minio
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
    LABEL = "label"


LAYER_PROPERTIES_LOOKUP = collections.OrderedDict((
    ("Active Covid-19 Cases by L7 Hex", (
        LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Hex ID", "Presumed Active Cases"),
        ("OrRd",), epi_map_case_layers_to_minio.HEX_L7_COUNT_SUFFIX, True, True,
        epi_map_case_layers_to_minio.ACTIVE_METADATA_KEY, True
    )),
    ("Active Covid-19 Cases by Ward", (
        LayerType.CHOROPLETH,
        (WARD_COUNT_NAME_PROPERTY, epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Ward Name", "Presumed Active Cases"),
        ("BuPu",), epi_map_case_layers_to_minio.WARD_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.ACTIVE_METADATA_KEY, True
    )),
    ("Active Covid-19 Cases by District", (
        LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Healthcare District Name", "Presumed Active Cases"),
        ("YlGn",), epi_map_case_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.ACTIVE_METADATA_KEY, True
    )),
    ("All Covid-19 Cases by L7 Hex", (
        LayerType.CHOROPLETH,
        (HEX_COUNT_INDEX_PROPERTY, epi_map_case_layers_to_minio.CASE_COUNT_COL), ("Hex ID", "All Cases"),
        ("OrRd",), epi_map_case_layers_to_minio.HEX_L7_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.CUMULATIVE_METADATA_KEY, True
    )),
    ("All Covid-19 Cases by Ward", (
        LayerType.CHOROPLETH,
        (WARD_COUNT_NAME_PROPERTY, epi_map_case_layers_to_minio.CASE_COUNT_COL), ("Ward Name", "All Cases"),
        ("BuPu",), epi_map_case_layers_to_minio.WARD_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.CUMULATIVE_METADATA_KEY, True
    )),
    ("All Covid-19 Cases by District", (
        LayerType.CHOROPLETH,
        (DISTRICT_NAME_PROPERTY, epi_map_case_layers_to_minio.ACTIVE_CASE_COUNT_COL),
        ("Healthcare District Name", "Presumed Active Cases"),
        ("YlGn",), epi_map_case_layers_to_minio.DISTRICT_COUNT_SUFFIX, False, True,
        epi_map_case_layers_to_minio.CUMULATIVE_METADATA_KEY, True
    )),
    ("Informal Settlements", (
        LayerType.POLYGON,
        ("INF_STLM_NAME",), ("Informal Settlement Name",),
        ("purple",), "informal_settlements.geojson", False, False, None, False
    )),
    ("Healthcare Facilities", (
        LayerType.POINT,
        ("NAME", "ADR",), ("Healthcare Facility Name", "Address",),
        ("red", "clinic-medical"), "health_care_facilities.geojson", False, False, None, False
    )),
    ("Healthcare Districts", (
        LayerType.POLYGON,
        ("CITY_HLTH_RGN_NAME",), ("Healthcare District Name",),
        ("red",), "health_districts.geojson", False, False, None, False
    )),
    ("Official Suburbs", (
        LayerType.POLYGON,
        ("OFC_SBRB_NAME",), ("Official Suburb Name",),
        ("black",), "official_suburbs.geojson", False, False, None, False
    )),
    ("Official Suburb Labels", (
        LayerType.LABEL,
        ("OFC_SBRB_NAME",), ("Official Suburb Name",),
        ("black",), "official_suburb_labels.geojson", False, False, None, False
    )),
))

BIN_QUANTILES = [0, 0, 0.5, 0.75, 0.9, 0.99, 1]

MAP_ZOOM = 9
MAP_RIGHT_PADDING = 100
MAP_FILENAME = "map_widget.html"


@functools.lru_cache(maxsize=1)
def _fetch_layer(tempdir, layer_filename_prefix, layer_suffix, apply_prefix, has_metadata,
                 minio_path_prefix, minio_access, minio_secret):
    layer_filename = (f"{layer_filename_prefix}_{layer_suffix}"
                      if apply_prefix
                      else layer_suffix)

    local_path = os.path.join(tempdir, layer_filename)

    layer_minio_path = (
        f"{minio_path_prefix}"
        f"{layer_filename}"
    )
    logging.debug(layer_minio_path)
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
            f"{minio_path_prefix}"
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

    return local_path, layer_gdf, layer_metadata


def get_layers(tempdir, minio_access, minio_secret,
               layer_properties=LAYER_PROPERTIES_LOOKUP,
               layer_filename_prefix=None,
               minio_path_prefix=epi_map_case_layers_to_minio.CASE_MAP_PREFIX):
    for layer, layer_properties in layer_properties.items():
        layer_type, *_, layer_suffix, _1, has_metadata, _3, apply_prefix = layer_properties

        local_path, layer_gdf, layer_metadata = _fetch_layer(
            tempdir, layer_filename_prefix, layer_suffix, apply_prefix, has_metadata,
            minio_path_prefix, minio_access, minio_secret
        )

        yield layer, local_path, layer_gdf, layer_metadata


def _get_choropleth_bins(count_series, bin_quantiles):
    bins = [count_series.min() - 1]
    data_edges = list(count_series.quantile(bin_quantiles).values)

    bins += [
        val for val in data_edges if val > bins[-1]
    ]

    # Adding extra padding if we have a bi-modal plot (colorbrewer needs 3 bins, at least)
    if len(bins) <= 3:
        bins.insert(0, count_series.min() - 1)

    return bins


def generate_map_features(layers_dict,
                          layer_properties=LAYER_PROPERTIES_LOOKUP,
                          float_left_offset="0%", choropleth_bins=BIN_QUANTILES,
                          properties_prefix=None, metadata_override=None, metadata_text_label_override=None):
    # Going layer by layer
    for title, (layer_path, count_gdf, layer_metadata) in layers_dict.items():
        (layer_type,
         layer_lookup_fields, layer_lookup_aliases,
         display_properties, layer_suffix, visible_by_default,
         has_metadata, metadata_key, apply_prefix) = layer_properties[title]
        logging.debug(f"Generating {title} map feature")

        # Everything gets packed into a feature group
        layer_feature_group = folium.features.FeatureGroup(
            name=f"{title}",
            show=visible_by_default
        )
        *_, layer_filename = os.path.split(layer_path)

        if layer_type in {LayerType.CHOROPLETH, LayerType.POLYGON}:
            layer_lookup_key, choropleth_key = layer_lookup_fields[:2] if layer_type is LayerType.CHOROPLETH else (
                None, None,)
            choropleth_key = f"{properties_prefix}-{choropleth_key}" if properties_prefix else choropleth_key

            colour_scheme, *_ = display_properties

            invert_colour_scheme = display_properties[1] if len(display_properties) > 1 else False
            if invert_colour_scheme:
                count_gdf[choropleth_key] = -1 * count_gdf[choropleth_key]

            # Folium is fussy about NaN types it gets served
            if layer_type is LayerType.CHOROPLETH:
                count_gdf[choropleth_key].fillna(numpy.nan, inplace=True)

            bins = None
            # Using hardcoded bins
            if len(display_properties) > 2 and layer_type is LayerType.CHOROPLETH:
                bins = -1*numpy.array(display_properties[2]) if invert_colour_scheme else display_properties[2]
                bins = sorted(bins)
            # Using percentile bins
            elif layer_type is LayerType.CHOROPLETH:
                bins = _get_choropleth_bins(count_gdf[choropleth_key].dropna(), choropleth_bins)

            if bins is not None:
                logging.debug(f"{str(layer_lookup_key)}, {str(choropleth_key)}")
                logging.debug(f"\n{count_gdf[choropleth_key].head(5)}")
                logging.debug(f"\n{count_gdf[choropleth_key].describe()}")
                logging.debug(f"bins={', '.join(map(str, bins))}")

            if "level_0" in count_gdf.columns:
                count_gdf.drop(columns=["level_0"], inplace=True)

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
                bins=bins,
                nan_fill_opacity=0,
                fill_opacity=0.7,
            ) if layer_type is LayerType.CHOROPLETH else folium.features.Choropleth(
                layer_path,
                name=title,
                show=visible_by_default,
                fill_opacity=0,
                line_color=colour_scheme,
            )

            # Monkey patching the choropleth GeoJSON to *not* embed
            choropleth.geojson.embed = False
            choropleth.geojson.embed_link = f"{layer_filename}"

            # Adding the hover-over tooltip
            if properties_prefix:
                layer_lookup_fields = [
                    (field if field == layer_lookup_key or not properties_prefix else f"{properties_prefix}-{field}")
                    for field in layer_lookup_fields
                ]

            layer_tooltip = folium.features.GeoJsonTooltip(
                fields=layer_lookup_fields,
                aliases=layer_lookup_aliases
            )
            choropleth.geojson.add_child(layer_tooltip)

            layer_feature_group.add_child(choropleth.geojson)
        elif layer_type is LayerType.POINT:
            colour, icon = display_properties

            marker_callback = f"""
            function(feature) {{
                var coords = feature.geometry.coordinates;

                var icon = L.AwesomeMarkers.icon({{
                    icon: "{icon}", prefix: "fa", markerColor: "{colour}"
                }});

                marker = L.marker(new L.LatLng(coords[1], coords[0]));
                marker.setIcon(icon);

                return marker;
            }};"""

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
                callback=marker_callback, tooltip_callback=tooltip_callback,
                name=f"{title}", show=visible_by_default
            )
            markers.embed = False
            markers.embed_link = layer_filename

            layer_feature_group.add_child(markers)
        elif layer_type is LayerType.LABEL:
            colour, *_ = display_properties
            layer_lookup_key, *_ = layer_lookup_fields

            marker_callback = f"""
                        function(feature) {{
                            var coords = feature.geometry.coordinates;
                            var marker = L.marker(
                              new L.LatLng(coords[1], coords[0]),
                              {{zIndex: 9999, opacity: 0}},
                            );
                            marker.bindTooltip(
                              feature.properties["{layer_lookup_key}"], 
                              {{permanent: true, offset: [0, 0], opacity: 0.7, direction: "center"}}
                            );

                            return marker;
                        }};"""

            markers = geojson_markers.GeoJsonMarkers(
                count_gdf.reset_index(), embed=True,
                callback=marker_callback, tooltip=False,
                name=f"{title}", show=visible_by_default
            )
            markers.embed = False
            markers.embed_link = layer_filename

            layer_feature_group.add_child(markers)

        # If this is a visible layer, calculating the centroids
        centroids = list(count_gdf.geometry.map(
            lambda shape: (shape.centroid.y, shape.centroid.x)
        )) if visible_by_default else []

        # Adding missing count from metadata
        metadata_key = metadata_override if metadata_override else metadata_key

        if metadata_key in layer_metadata:
            not_displayed = layer_metadata[metadata_key][epi_map_case_layers_to_minio.NOT_SPATIAL_CASE_COUNT]
            total_count = layer_metadata[metadata_key][epi_map_case_layers_to_minio.CASE_COUNT_TOTAL]

            metadata_text_label = metadata_text_label_override if metadata_text_label_override else "Cases not displayed"
            metadata_text = f"{metadata_text_label}: {not_displayed} ({not_displayed / total_count:.1%} of total)"
            div = float_div.FloatDiv(content=(
                "<span style='font-size: 20px; color:#FF0000'>" +
                 metadata_text +
                "</span>"
            ), top="95%", left=float_left_offset)
            layer_feature_group.add_child(div)

        yield layer_feature_group, centroids


def generate_map(map_features, map_zoom=MAP_ZOOM, map_right_padding=MAP_RIGHT_PADDING, add_basemap=True):
    m = folium.Map(
        location=CITY_CENTRE, zoom_start=map_zoom,
        tiles="",
        prefer_canvas=True
    )

    # Feature Map
    if add_basemap:
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
    if map_centroids:
        m.fit_bounds(map_centroids, padding_bottom_right=(0, map_right_padding), max_zoom=map_zoom)

    return m


def add_layer_control_to_map(map):
    # Layer Control
    layer_control = folium.LayerControl(collapsed=False)
    layer_control.add_to(map)

    return map


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
            f"{epi_map_case_layers_to_minio.CASE_MAP_PREFIX}"
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


def pull_out_leaflet_deps(tempdir, proxy_username, proxy_password, minio_access, minio_secret,
                          extra_js_deps=[], extra_css_deps=[]):
    http_session = requests.Session()

    proxy_string = f'http://{proxy_username}:{proxy_password}@{CITY_PROXY_DOMAIN}/'
    http_session.proxies = {
        "http": proxy_string,
        "https": proxy_string
    }

    js_libs = [
        (key, get_leaflet_dep_file(url, tempdir, http_session, minio_access, minio_secret))
        for key, url in (folium.folium._default_js + extra_js_deps)
    ]

    css_libs = [
        (key, get_leaflet_dep_file(url, tempdir, http_session, minio_access, minio_secret))
        for key, url in (folium.folium._default_css + extra_css_deps)
    ]

    return js_libs, css_libs


def write_map_to_minio(city_map, map_file_prefix, tempdir, minio_access, minio_secret,
                       js_libs, css_libs, map_suffix=MAP_FILENAME,
                       map_minio_prefix=epi_map_case_layers_to_minio.CASE_MAP_PREFIX):
    # Uploading JS and CSS dependencies
    for key, new_path in js_libs + css_libs:
        *_, local_filename = os.path.split(new_path)
        local_path = os.path.join(tempdir, local_filename)

        logging.debug(f"Uploading '{key}' from '{local_path}'")
        result = minio_utils.file_to_minio(
            filename=local_path,
            filename_prefix_override=f"{map_minio_prefix}{DEP_DIR}/",
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        assert result, f"Failed to upload {local_filename}"

    map_filename = f"{map_file_prefix}_{map_suffix}"
    local_path = os.path.join(tempdir, map_filename)

    folium.folium._default_js = js_libs
    folium.folium._default_css = css_libs

    city_map.save(local_path)

    result = minio_utils.file_to_minio(
        filename=local_path,
        filename_prefix_override=map_minio_prefix,
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
            for layer, local_path, layer_gdf, layer_metadata in get_layers(tempdir,
                                                                           secrets["minio"]["edge"]["access"],
                                                                           secrets["minio"]["edge"]["secret"],
                                                                           layer_filename_prefix=f"{district_file_prefix}_{subdistrict_file_prefix}", )
        }
        logging.info("G[ot] layers")

        logging.info("Generat[ing] map")
        map_feature_generator = generate_map_features(map_layers_dict)
        data_map = generate_map(map_feature_generator)
        data_map = add_layer_control_to_map(data_map)
        logging.info("Generat[ed] map")

        logging.info("Writ[ing] to Minio")
        write_map_to_minio(data_map,
                           f"{district_file_prefix}_{subdistrict_file_prefix}",
                           tempdir,
                           secrets["minio"]["edge"]["access"],
                           secrets["minio"]["edge"]["secret"],
                           js_libs, css_libs)
        logging.info("Wr[ote] to Minio")
