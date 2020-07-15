import itertools
import json
import os
import pprint
import logging
import sys
import tempfile

from db_utils import minio_utils
import pandas

import city_map_layers_to_minio
from city_map_widget_to_minio import CITY_PROXY_DOMAIN
import table_widget

SUBDISTRICT_POP_FILE = "subdistrict_populations.csv"
SUBDISTRICT_POP_COL = "TotalPopulation"

CASES_TOTAL_COL = "CasesTotal"
ACTIVE_CASES_COL = "ActiveCases"
HOSPITAL_CASES_COL = "Hospitalised"
FATAL_CASES_COL = "Died"
STAT_COLS = (CASES_TOTAL_COL, ACTIVE_CASES_COL, HOSPITAL_CASES_COL, FATAL_CASES_COL)

REPORTING_DELAY = pandas.Timedelta(days=2)
ACTIVE_DELAY = pandas.Timedelta(days=14)

PER_CAPITA_SUFFIX = "PerCapita"
DELTA_COL_SUFFIX = "Delta"

sort_function_template = """
function sortCompare(a, b){{
    let re = {delta_regex};
    var aFloat = parseFloat(a.match(re)[1]);
    var bFloat = parseFloat(b.match(re)[1]);
    if (aFloat > bFloat){{
        return 1;
    }} else if (bFloat > aFloat){{
        return -1;
    }} else {{
        return 0;
    }}
}}
"""
NEW_MIN_VALUE = 0.7
NEW_MAX_VALUE = 1.0
number_cell_style_function_template = """
function {formatter_name}(cell){{
    let re = {delta_regex};
    var cellFloat = parseFloat(cell.match(re)[1]);
    var remappedCellValue = {new_min_value} + (cellFloat - ({old_min_value}))*{new_value_delta}/({old_value_delta})
    var backgroundSize = Math.round(remappedCellValue*100) + "%";

    var backgroundColour = 'lightgrey'
    var textColour = 'black'
    if (cellFloat <= {bottom_range}) {{
        backgroundColour = 'green'
        textColour = 'white'
    }} else if (cellFloat >= {top_range}) {{
        backgroundColour = 'red'
        textColour = 'white'
    }}

    var styleString = 'width:' + backgroundSize + ';background-color:' + backgroundColour + ';color:' + textColour + ';display:block;overflow:auto;'
    styleString += "padding-left:3px;padding-right:3px;"
    styleString += "padding-top:12px;padding-bottom:12px;"
    return gridjs.html('<span style=' + styleString + '><b>' + cell + '</b></span>')
}};
"""
row_header_cell_style_function_template = """
function {formatter_name}(cell){{
    styleString = "padding-left:3px;padding-right:3px;"
    styleString += "padding-top:12px;padding-bottom:12px;"
    return gridjs.html('<span style=' + styleString + '><b>' + cell + '</b></span>')
}};
"""

STATS_WIDGET_NAME = "covid19_subdistrict_stats_table"
STATS_WIDGET_TITLE = "City of Cape Town Covid-19 Summary Statistics"
STATS_WIDGET_PREFIX = "widgets/private/subdistrict_stats_table_widgets/"


def get_subdistrict_populations(minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_datafile:
        minio_utils.minio_to_file(
            filename=temp_datafile.name,
            minio_filename_override=city_map_layers_to_minio.DATA_RESTRICTED_PREFIX + SUBDISTRICT_POP_FILE,
            minio_bucket=city_map_layers_to_minio.MINIO_COVID_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=city_map_layers_to_minio.MINIO_CLASSIFICATION,
        )

        population_df = pandas.read_csv(temp_datafile.name)
        population_df.set_index(city_map_layers_to_minio.SUBDISTRICT_COL, inplace=True)

    return population_df


def count_by_subdistrict(filter_cases_df, time_filter_col=None, filter_date_start=None, filter_date_end=None):
    if time_filter_col is not None:
        filter_date_start = filter_date_start if filter_date_start else filter_cases_df[time_filter_col].min()

        time_filter = filter_cases_df[time_filter_col] >= filter_date_start
        time_filter &= (filter_cases_df[time_filter_col] < filter_date_end) if filter_date_end else True

        logging.debug(f"time_filter.sum()={time_filter.sum()}")
        logging.debug(f"cases_df.shape={filter_cases_df.shape}")
        filter_cases_df = filter_cases_df[time_filter]
        logging.debug(f"cases_df.shape={filter_cases_df.shape}")

    return filter_cases_df.groupby(city_map_layers_to_minio.SUBDISTRICT_COL).apply(
        lambda subdistrict_df: pandas.Series({
            CASES_TOTAL_COL: subdistrict_df.shape[0],
            HOSPITAL_CASES_COL: (subdistrict_df[HOSPITAL_CASES_COL] == "Yes").sum(),
            FATAL_CASES_COL: (subdistrict_df[FATAL_CASES_COL] == "Yes").sum()
        })
    )


def get_subdistrict_stats(all_cases_df, subdistrict_pop_df):
    # Total cases, Hospitalised, Died
    stats_df = count_by_subdistrict(all_cases_df)

    # Active cases
    active_start_date = cases_df[city_map_layers_to_minio.DATE_DIAGNOSIS_COL].max() - ACTIVE_DELAY
    active_start_date -= REPORTING_DELAY
    logging.debug(f"Using '{active_start_date}' as presumed active start date")
    active_df = count_by_subdistrict(all_cases_df,
                                     time_filter_col=city_map_layers_to_minio.DATE_DIAGNOSIS_COL,
                                     filter_date_start=active_start_date)
    stats_df[ACTIVE_CASES_COL] = active_df[CASES_TOTAL_COL]

    # Per Capita
    for col in STAT_COLS:
        stats_df[col + PER_CAPITA_SUFFIX] = stats_df[col] / subdistrict_pop_df[SUBDISTRICT_POP_COL]

    # Previous week's stats
    previous_week = cases_df[city_map_layers_to_minio.DATE_DIAGNOSIS_COL].max() - pandas.Timedelta(weeks=1)
    previous_week -= REPORTING_DELAY
    logging.debug(f"Using '{previous_week}' as start of previous week")
    previous_df = count_by_subdistrict(all_cases_df,
                                       time_filter_col=city_map_layers_to_minio.DATE_DIAGNOSIS_COL,
                                       filter_date_end=previous_week)

    previous_week_active_start = previous_week - ACTIVE_DELAY
    logging.debug(f"Using '{previous_week_active_start}' as start of previous week's presumed active period")
    active_df = count_by_subdistrict(all_cases_df,
                                     time_filter_col=city_map_layers_to_minio.DATE_DIAGNOSIS_COL,
                                     filter_date_start=previous_week_active_start,
                                     filter_date_end=previous_week)
    previous_df[ACTIVE_CASES_COL] = active_df[CASES_TOTAL_COL]

    for col in STAT_COLS:
        previous_df[col + PER_CAPITA_SUFFIX] = previous_df[col] / subdistrict_pop_df[SUBDISTRICT_POP_COL]

    # Calculating deltas
    delta_df = stats_df - previous_df
    stats_df = stats_df.merge(
        delta_df,
        suffixes=("", DELTA_COL_SUFFIX),
        left_index=True, right_index=True
    )

    return stats_df


def generate_table_widget_data_df(stats_df):
    stats_df = stats_df.copy().astype(int)
    logging.debug(f"stats_df=\n{stats_df}")

    output_df = stats_df.sort_values(by=CASES_TOTAL_COL + DELTA_COL_SUFFIX, ascending=False).apply(
        lambda row: pandas.Series({
            "Total Cases (w/w)": f"{row[CASES_TOTAL_COL]} ({row[CASES_TOTAL_COL + DELTA_COL_SUFFIX]:+d})",
            "Active (w/w)": f"{row[ACTIVE_CASES_COL]} ({row[ACTIVE_CASES_COL + DELTA_COL_SUFFIX]:+d})",
            "Hospitalised (w/w)": f"{row[HOSPITAL_CASES_COL]} ({row[HOSPITAL_CASES_COL + DELTA_COL_SUFFIX]:+d})",
            "Fatalities (w/w)": f"{row[FATAL_CASES_COL]} ({row[FATAL_CASES_COL + DELTA_COL_SUFFIX]:+d})",
        }),
        axis=1
    )

    return output_df


def generate_per_capita_table_widget_data_df(stats_df):
    stats_df = stats_df.copy()

    logging.debug(f"stats_df=\n{stats_df}")
    for col in STAT_COLS:
        stats_df[col + PER_CAPITA_SUFFIX] = (stats_df[col + PER_CAPITA_SUFFIX] * 1e5).round(0).astype(int)
        stats_df[col + PER_CAPITA_SUFFIX + DELTA_COL_SUFFIX] = (
            (stats_df[col + PER_CAPITA_SUFFIX + DELTA_COL_SUFFIX] * 1e5).round(0).astype(int)
        )

    output_df = stats_df.sort_values(by=CASES_TOTAL_COL + DELTA_COL_SUFFIX, ascending=False).apply(
        lambda row: pandas.Series({
            "Total per 100k (w/w)": f"{row[CASES_TOTAL_COL + PER_CAPITA_SUFFIX]} "
                                    f"({row[CASES_TOTAL_COL + PER_CAPITA_SUFFIX + DELTA_COL_SUFFIX]:+d})",
            "Active per 100k (w/w)": f"{row[ACTIVE_CASES_COL + PER_CAPITA_SUFFIX]} "
                                     f"({row[ACTIVE_CASES_COL + PER_CAPITA_SUFFIX + DELTA_COL_SUFFIX]:+d})",
            "Hospitalised per 100k (w/w)": f"{row[HOSPITAL_CASES_COL + PER_CAPITA_SUFFIX]} "
                                           f"({row[HOSPITAL_CASES_COL + PER_CAPITA_SUFFIX + DELTA_COL_SUFFIX]:+d})",
            "Fatalities per 100k (w/w)": f"{row[FATAL_CASES_COL + PER_CAPITA_SUFFIX]} "
                                         f"({row[FATAL_CASES_COL + PER_CAPITA_SUFFIX + DELTA_COL_SUFFIX]:+d})",
        }),
        axis=1
    )

    return output_df


def generate_table_widget_data(table_data_df):
    # Converting df to JSON - this forms the basis of the table widget data
    output_data = table_data_df.reset_index().to_dict(orient='split')
    del output_data["index"]

    # table level options
    output_data["sort"] = True
    output_data["style"] = {"td": {"padding": "0px"}}

    # Applying column level sorting attributes
    for i, col in enumerate(output_data["columns"]):
        new_col_dict = {
            "name": col,
            "sort": {
                "compare": "sortCompare"
            },
        }
        output_data["columns"][i] = new_col_dict

    # Custom sort function, for sorting on delta values
    delta_regex_pattern = r'/\(([+-]*\d+)\)/'
    sort_func = sort_function_template.format(delta_regex=delta_regex_pattern)

    # Applying column level styling attributes
    # This is pretty brittle - could probably do with a refactor
    style_funcs = []
    for col_dict in output_data['columns']:
        logging.debug(f"Styling '{col_dict['name']}'")
        formatter_name = col_dict['name'].replace(" ", "").replace("(w/w)", "") + "Formatter"
        if col_dict['name'] != city_map_layers_to_minio.SUBDISTRICT_COL:
            col_delta_values = sorted(
                table_data_df[col_dict['name']].str.extract("\(([+-]*\d+)\)").astype(float).values
            )
            sorted_vals = [val for array in col_delta_values for val in array]
            logging.debug(f"sorted_vals={','.join(map(str, sorted_vals))}")

            bottom_two = sorted_vals[1]
            top_two = sorted_vals[-2]
            min_value = sorted_vals[0]
            max_value = sorted_vals[-1]
            logging.debug(f"bottom_two={bottom_two}, top_two={top_two}")
            logging.debug(f"min_value={min_value}, max_value={max_value}")

            style_func = number_cell_style_function_template.format(
                formatter_name=formatter_name,
                delta_regex=delta_regex_pattern,
                new_min_value=NEW_MIN_VALUE, new_value_delta=(NEW_MAX_VALUE - NEW_MIN_VALUE),
                old_min_value=min_value, old_value_delta=(max_value - min_value),
                bottom_range=bottom_two, top_range=top_two
            )
        else:
            style_func = row_header_cell_style_function_template.format(formatter_name=formatter_name, )

        col_dict["formatter"] = formatter_name
        style_funcs += [style_func]

    logging.debug(f"output_data=\n{pprint.pformat(output_data)}")
    return output_data, [sort_func] + style_funcs


def create_table_widget_generator(district_prefix, output_data, js_funcs, proxy_username, proxy_password, extra_suffix=None):
    widget_name_components = [district_prefix]
    widget_name_components += [extra_suffix] if extra_suffix else []
    widget_name_components += [STATS_WIDGET_NAME]

    stats_widget_name = "_".join(widget_name_components)

    tw = table_widget.TableWidget(output_data, stats_widget_name, STATS_WIDGET_TITLE, js_funcs=js_funcs)

    return tw.output_file_generator(proxy_username, proxy_password, CITY_PROXY_DOMAIN)


def write_table_widget_file_to_minio(localpath, minio_access, minio_secret):
    result = minio_utils.file_to_minio(
        filename=localpath,
        filename_prefix_override=STATS_WIDGET_PREFIX,
        minio_bucket=city_map_layers_to_minio.MINIO_COVID_BUCKET,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=city_map_layers_to_minio.MINIO_CLASSIFICATION,
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

    district_file_prefix = sys.argv[1]
    district_name = sys.argv[2]

    logging.info(f"Generat[ing] stats table for '{district_name}' district")

    logging.info("Gett[ing] case data")
    cases_df = city_map_layers_to_minio.get_case_data(secrets["minio"]["edge"]["access"],
                                                      secrets["minio"]["edge"]["secret"])
    logging.info("G[ot] case data")

    logging.info("Filter[ing] to District Level")
    filtered_df = city_map_layers_to_minio.filter_district_case_data(cases_df, district_name, "*")
    logging.info("Filter[ed] to District Level")

    logging.info("Gett[ing] population data")
    subdistrict_population_df = get_subdistrict_populations(secrets["minio"]["edge"]["access"],
                                                            secrets["minio"]["edge"]["secret"])
    logging.info("G[ot] population data")

    logging.info("Count[ing] by subdistrict")
    subdistrict_df = get_subdistrict_stats(filtered_df, subdistrict_population_df)
    logging.info("Count[ed] by subdistrict")

    logging.info("Generat[ing] table widget dataframe")
    table_widget_data_df = generate_table_widget_data_df(subdistrict_df)
    per_capita_table_widget_data_df = generate_per_capita_table_widget_data_df(subdistrict_df)
    logging.info("Generat[ed] table widget dataframe")

    logging.info("Generat[ing] table widget data")
    table_widget_data, table_widget_js_funcs = generate_table_widget_data(table_widget_data_df)
    per_capita_table_widget_data, per_capita_table_widget_js_funcs = generate_table_widget_data(per_capita_table_widget_data_df)
    logging.info("Generat[ed] table widget data")

    table_widget_generators = itertools.chain(
        create_table_widget_generator(district_file_prefix, table_widget_data, table_widget_js_funcs,
                                      secrets["proxy"]["username"], secrets["proxy"]["password"]),
        create_table_widget_generator(district_file_prefix,
                                      per_capita_table_widget_data, per_capita_table_widget_js_funcs,
                                      secrets["proxy"]["username"], secrets["proxy"]["password"],
                                      extra_suffix="per_capita"),
    )

    for tw_filename, tw_file_localpath in table_widget_generators:
        logging.info(f"Upload[ing] {tw_filename} to Minio")
        write_table_widget_file_to_minio(tw_file_localpath,
                                         secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
        logging.info(f"Upload[ed] {tw_filename} to Minio")

    logging.info("Done!")
