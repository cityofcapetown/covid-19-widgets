import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import folium
import geopandas
import pandas

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_PUBLIC_PREFIX = "data/public"
DATA_RESTRICTED_PREFIX = "data/private/"
WIDGETS_RESTRICTED_PREFIX = "widgets/private/city_"

CITY_CASE_DATA_FILENAME = "wc_all_cases.csv"
CITY_CASE_DATA_SUBDISTRICT_COL = "subdistrict"
HEALTH_DISTRICT_FILENAME = "health_districts.geojson"
HEALTH_DISTRICT_SUBDISTRICT_PROPERTY = "CITY_HLTH_RGN_NAME"
LAYER_FILES = (
    "health_districts.geojson",
    "informal_settlements.geojson",
    "health_care_facilities.geojson"
)
SUBDISTRICT_COUNT_FILENAME = "city_subdistrict_case_count.geojson"

CITY_CENTRE = (-33.9715, 18.6021)
LAYER_PROPERTIES_LOOKUP = {
    SUBDISTRICT_COUNT_FILENAME: (("subdistrict", "OBJECTID"), ("Subdistrict", "Case Count")),
    "informal_settlements.geojson": (("INF_STLM_NAME",), ("Informal Settlement Name",)),
    "health_care_facilities.geojson": (("NAME", "ADR",), ("Healthcare Facility Name", "Address",)),
}
MAP_FILENAME = "city_map.html"


def get_layers(tempdir, minio_access, minio_secret):
    for layer in LAYER_FILES:
        local_path = os.path.join(tempdir, layer)

        minio_utils.minio_to_file(
            filename=local_path,
            minio_filename_override=f"data/public/{layer}",
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        layer_gdf = geopandas.read_file(local_path)

        return layer, local_path, layer_gdf


def get_case_data(minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_datafile:
        minio_utils.minio_to_file(
            filename=temp_datafile.name,
            minio_filename_override=CITY_CASE_DATA_FILENAME,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        case_data_df = pandas.read(temp_datafile.name)

    return case_data_df


def spatialise_case_data(case_data_df, health_district_data_gdf):
    join_col = "join_col"
    case_data_df[join_col] = case_data_df[CITY_CASE_DATA_SUBDISTRICT_COL].str.upper()

    case_data_gdf = health_district_data_gdf.merge(
        case_data_df,
        right_on=join_col,
        left_on=HEALTH_DISTRICT_SUBDISTRICT_PROPERTY
    )

    return case_data_gdf


def get_district_case_counts(case_data_gdf):
    # Doing this operation spacialy is unnecessary, but it saves hassle around reapplying the geometry column,
    # which gets lost when you do a standard groupby...
    distrct_count_gdf = case_data_gdf.dissolve(HEALTH_DISTRICT_SUBDISTRICT_PROPERTY)

    return distrct_count_gdf


def write_district_count_gdf_to_disk(district_count_data_gdf, tempdir):
    local_path = os.path.join(tempdir, SUBDISTRICT_COUNT_FILENAME)
    district_count_data_gdf.to_file(local_path, driver='GeoJSON')

    return SUBDISTRICT_COUNT_FILENAME, (local_path, district_count_data_gdf)


def write_layers_to_minio(layers_dict, minio_access, minio_secret):
    for layer_name, (layer_local_path, _) in layers_dict.items():
        result = minio_utils.file_to_minio(
            filename=layer_local_path,
            filename_prefix_override=WIDGETS_RESTRICTED_PREFIX,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        assert result


def generate_map(layers_dict):
    m = folium.Map(
        location=CITY_CENTRE, zoom_start=9,
        tiles="",
        prefer_canvas=True
    )

    # Feature Map
    m.add_child(
        folium.TileLayer(
            name='Feature',
            tiles='https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
            attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; '
                 '<a href="https://carto.com/attributions">CARTO</a>'
        )
    )

    choropleths = []

    # Adding Covid Case count choropleth
    subdistrict_count_local_path, subdistrict_count_gdf = layers_dict[SUBDISTRICT_COUNT_FILENAME]
    choropleths += [folium.features.Choropleth(
        subdistrict_count_local_path,
        data=subdistrict_count_gdf,
        name="Covid-19 Case Count",
        key_on=f"feature.properties.{CITY_CASE_DATA_SUBDISTRICT_COL}",
        columns=[CITY_CASE_DATA_SUBDISTRICT_COL, HEALTH_DISTRICT_SUBDISTRICT_PROPERTY],
        fill_color='BuPu',
        highlight=True,
    )]

    # Adding all of the other layers
    # ToDo use layer colours to style the map - I like quintiles
    for layer in layers_dict:
        if layer != SUBDISTRICT_COUNT_FILENAME:
            layer_local_path, _ = map_layers_dict[layer]
            layer_name = layer.replace("_", " ").replace(".geojson", "").title()
            choropleths += [folium.features.Choropleth(
                layer_local_path,
                name=layer_name,
                show=False
            )]

    # Styling them there choropleths.
    for layer, choropleth in zip(layers_dict, choropleths):
        # Monkey patching the choropleth GeoJSON to *not* embed
        choropleth.geojson.embed = False
        choropleth.geojson.embed_link = layer

        # Adding the hover-over tooltip
        if layer in LAYER_PROPERTIES_LOOKUP:
            layer_lookup_fields, layer_lookup_aliases = LAYER_PROPERTIES_LOOKUP[layer]
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
    logging.basicConfig(level=logging.DEBUG,
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
            layer: (local_path, layer_gdf)
            for layer, local_path, layer_gdf in get_layers(tempdir,
                                                           secrets["minio"]["edge"]["access"],
                                                           secrets["minio"]["edge"]["secret"])
        }
        logging.info("G[ot] layers")

        logging.info("G[etting] Case Data")
        cases_df = get_case_data(secrets["minio"]["edge"]["access"],
                                 secrets["minio"]["edge"]["secret"])
        logging.info("G[ot] Case Data")

        logging.info("Spatialis[ing] case data")
        _, health_district_gdf = map_layers_dict[HEALTH_DISTRICT_FILENAME]
        cases_gdf = spatialise_case_data(cases_df, health_district_gdf)
        logging.info("Spatialis[ed] case data")

        logging.info("Count[ing] cases per district")
        district_count_gdf = get_district_case_counts(cases_gdf)
        logging.info("Count[ed] cases per district")

        logging.info("Writ[ing] layers to Minio")
        count_layer_name, count_layer_values = write_district_count_gdf_to_disk(district_count_gdf, tempdir)
        map_layers_dict[count_layer_name] = count_layer_values
        write_layers_to_minio(map_layers_dict,
                              secrets["minio"]["edge"]["access"],
                              secrets["minio"]["edge"]["secret"])
        logging.info("Wr[ote] layers to Minio")

        logging.info("Generat[ing] map")
        data_map = generate_map(map_layers_dict)
        logging.info("Generat[ed] map")

        logging.info("Writ[ing] to Minio")
        write_map_to_minio(data_map, tempdir,
                           secrets["minio"]["edge"]["access"],
                           secrets["minio"]["edge"]["secret"])
        logging.info("Wr[ote] to Minio")


