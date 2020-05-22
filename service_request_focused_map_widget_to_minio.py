import json
import logging
import os
import sys
import tempfile

import branca
from db_utils import minio_utils
import folium
from folium.plugins import FastMarkerCluster
import jinja2
import pandas

import service_request_map_layers_to_minio
import service_request_map_widget_to_minio

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

CITY_PROXY_DOMAIN = "internet.capetown.gov.za:8080"
DEP_DIR = "libdir"

TOP_REQUEST_NUMBER = 20
CITY_CENTRE = (-33.9715, 18.6021)

MAP_SUFFIX = "focused_map.html"

MARKER_CALLBACK = """
function (row) {
    var icon, marker, text, marker_icon, marker_colour;

    text = row[2];
    marker_icon = row[3];
    marker_colour = row[4];

    icon = L.AwesomeMarkers.icon({
        icon: marker_icon, prefix: "fa", markerColor: marker_colour
    });

    marker = L.marker(new L.LatLng(row[0], row[1]));
    marker.setIcon(icon);
    marker.bindPopup(text);

    return marker;
};
"""


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


def get_basemap():
    # Basemap
    m = folium.Map(
        location=CITY_CENTRE, zoom_start=9,
        tiles="",
        prefer_canvas=True
    )

    # Image Layer
    m.add_child(
        folium.TileLayer(
            name='Feature',
            tiles='https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
            attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
        )
    )

    # Terrain Map
    m.add_child(
        folium.TileLayer(
            name='Terrain',
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            attr='Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
        )
    )

    return m


def marker_data_generator(request_df):
    return (
        (
            row.Latitude, row.Longitude,
            f"{row.Index}: '{row.NotificationShortText}' (open {row.Duration / 86400:.1f} days)",
            *(
                ("wrench", "blue") if pandas.isna(row.CompletionTimestamp) else ("check", "green")
            )
            # pandas.isna(row.CompletionTimestamp)
        )
        for row in request_df.itertuples()
    )


def get_prototype_div(dept_geospatial_proportion, start_time, top=95):
    prototype_message = f"Displaying {dept_geospatial_proportion:.1%} of all requests, since {start_time.isoformat()}"

    div = FloatDiv(
        content="""
        <div style="font-size: 20px; color:#FFFFFF; background-color:#f00">
            {prototype_message}
        </div>
        """.format(prototype_message=prototype_message),
        top=top
    )

    return div


def get_dept_clusters(dept_df):
    # Constructing cluster list
    fast_marker_clusters_tuples = (
        (request_df.shape[0],
         FastMarkerCluster(name=request,
                           data=marker_data_generator(request_df),
                           callback=MARKER_CALLBACK,
                           options={'disableClusteringAtZoom': 17},
                           show=False))
        for request, request_df in dept_df.groupby(["Code"])
    )

    # Creating list of clusters, sorted by size
    fast_marker_clusters = map(
        lambda marker_tuple: marker_tuple[1],
        sorted(
            fast_marker_clusters_tuples,
            key=lambda marker_tuple: marker_tuple[0],
            reverse=True
        )[:TOP_REQUEST_NUMBER]
    )

    return fast_marker_clusters


def generate_map(map_data, total_requests, start_time):
    # Get map
    m = get_basemap()

    # Add clusters
    for i, cluster in enumerate(get_dept_clusters(map_data)):
        # Making first complaint type visible by default
        if i == 0:
            cluster.show = True

        cluster.add_to(m)

    # Add prototype banner
    dept_geospatial_proportion = map_data.shape[0] / total_requests

    dept_prototype_div = get_prototype_div(dept_geospatial_proportion, start_time)
    dept_prototype_div.add_to(m)

    # Add layer control last
    layer_control = folium.LayerControl(collapsed=False)
    layer_control.add_to(m)

    return m


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

    logging.info(f"Generat[ing] service request maps for '{directorate_title}'")

    logging.info("G[etting] SR Data")
    sr_data_df = service_request_map_layers_to_minio.get_service_request_data(
        secrets["minio"]["confidential"]["access"],
        secrets["minio"]["confidential"]["secret"])
    logging.info("G[ot] SR Data")

    logging.info("Upfront filter[ing] of SR Data")
    filter_df = service_request_map_layers_to_minio.filter_sr_data(
        sr_data_df, service_request_map_layers_to_minio.SOD_DATE,
        directorate_title if directorate_title != "*" else None,
        open_filter=False
    )
    logging.info("Upfront filter[ed] SR Data")

    # Map per time period
    map_prefixes = (
        (directorate_file_prefix, time_period_prefix)
        for time_period_prefix, _ in service_request_map_layers_to_minio.TIME_PERIODS
    )

    # Has to be in the outer scope as use the tempdir in multiple places
    with tempfile.TemporaryDirectory() as tempdir:
        logging.info("Fetch[ing] Folium dependencies")
        js_libs, css_libs = (
            service_request_map_widget_to_minio.pull_out_leaflet_deps(tempdir,
                                                                      secrets["proxy"]["username"],
                                                                      secrets["proxy"]["password"],
                                                                      secrets["minio"]["edge"]["access"],
                                                                      secrets["minio"]["edge"]["secret"])
        )
        logging.info("Fetch[ed] Folium dependencies")

        for time_period_prefix, time_period_date_func in service_request_map_layers_to_minio.TIME_PERIODS:
            for open in (True, False):
                logging.info(f"Generat[ing] {'open ' if open else ''}service requests map for '{directorate_title}' - "
                             f"'{time_period_prefix}'")

                time_period_start_date = time_period_date_func(filter_df)
                logging.debug(f"time_period_start_date={time_period_start_date.strftime('%Y-%m-%d')}")

                logging.info("G[etting] map data")
                # First filtering by time and open status
                time_period_filtered_df = service_request_map_layers_to_minio.filter_sr_data(filter_df,
                                                                                             time_period_start_date,
                                                                                             open_filter=open)
                total_time_period_requests = time_period_filtered_df.shape[0]
                if total_time_period_requests == 0:
                    logging.warning("Skipping because there are no requests for this time period")
                    continue

                # Then by space
                spatial_filtered_df = service_request_map_layers_to_minio.filter_sr_data(time_period_filtered_df,
                                                                                         time_period_start_date,
                                                                                         spatial_filter=True)
                logging.info("G[ot] map data")

                logging.info("Generat[ing] map")
                data_map = generate_map(spatial_filtered_df, total_time_period_requests, time_period_start_date)
                logging.info("Generat[ed] map")

                logging.info("Writ[ing] to Minio")
                map_suffix = f"open_{MAP_SUFFIX}" if open else MAP_SUFFIX
                service_request_map_widget_to_minio.write_map_to_minio(
                    data_map, directorate_file_prefix, time_period_prefix, map_suffix, tempdir,
                    secrets["minio"]["edge"]["access"],
                    secrets["minio"]["edge"]["secret"],
                    js_libs, css_libs)
                logging.info("Wr[ote] to Minio")
