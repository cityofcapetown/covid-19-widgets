import datetime
import json
import math
import logging
import os
import sys
import tempfile

from bokeh.embed import file_html
from bokeh.models import HoverTool, NumeralTickFormatter, DatetimeTickFormatter, Range1d, LinearAxis, Legend, RangeTool, \
    Span
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.resources import CDN
from db_utils import minio_utils
import holidays
import numpy
import pandas

import hr_data_last_values_to_minio
from hr_data_last_values_to_minio import WORKING_STATUS, NOT_WORKING_STATUS, directorate_filter_df, merge_df
from hr_bp_emailer import get_data_df, HR_MASTER_FILENAME_PATH, HR_TRANSACTIONAL_FILENAME_PATH

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_RESTRICTED_PREFIX = "data/private/"

DEPARTMENT_COL = "Department"
DATE_COL_NAME = "Date"
STATUS_COL = "Categories"
SUCCINCT_STATUS_COL = "SuccinctStatus"
COVID_SICK_COL = "CovidSick"
ABSENTEEISM_RATE_COL = "Absent"
DAY_COUNT_COL = "DayCount"

ROLLING_WINDOW = 7

TZ_STRING = "Africa/Johannesburg"
ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M"

WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
PLOT_FILENAME_SUFFIX = "hr_absenteeism_plot.html"


def filter_df(hr_df, query_string):
    return hr_df.query(query_string)


def make_statuses_succinct_again(hr_df):
    hr_df[SUCCINCT_STATUS_COL] = hr_df[STATUS_COL].apply(
        hr_data_last_values_to_minio.STATUSES_TO_SUCCINCT_MAP.get
    )
    logging.debug(f"hr_df.head(5)=\n{hr_df.head(5)}")

    no_succinct_status = hr_df[hr_df[SUCCINCT_STATUS_COL].isna()].Categories
    logging.debug(f"no_succinct_status.value_counts()=\n{no_succinct_status.value_counts()}")

    hr_df[COVID_SICK_COL] = hr_df[STATUS_COL].isin(hr_data_last_values_to_minio.COVID_STATUSES)
    logging.debug(f"hr_df[COVID_SICK_COL].head(5)=\n{hr_df[COVID_SICK_COL].head(5)}")

    return hr_df


def get_plot_df(succinct_hr_df):
    succinct_hr_df = succinct_hr_df.copy()
    succinct_hr_df[DATE_COL_NAME] = succinct_hr_df[DATE_COL_NAME].dt.date
    plot_df = (
        succinct_hr_df.groupby(
            DATE_COL_NAME
        ).apply(
            lambda data_df: pandas.DataFrame({
                ABSENTEEISM_RATE_COL: [
                    data_df[SUCCINCT_STATUS_COL].value_counts(normalize=True)[NOT_WORKING_STATUS]
                    if data_df[SUCCINCT_STATUS_COL].str.contains(NOT_WORKING_STATUS).any() else 0
                ],
                COVID_SICK_COL: [
                    data_df[COVID_SICK_COL].sum() / data_df[COVID_SICK_COL].shape[0]
                    if data_df[COVID_SICK_COL].shape[0] > 0 else 0
                ],
                DAY_COUNT_COL: data_df.shape[0]
            })
        )
    ).reset_index().drop("level_1", axis='columns')
    logging.debug(f"plot_df=\n{plot_df}")

    # Converting trend cols in 7 day rolling means
    def weighted_average(value_df, col):
        return numpy.average(
            plot_df.loc[value_df.index, col],
            weights=plot_df.loc[value_df.index, DAY_COUNT_COL]
        )

    plot_df[ABSENTEEISM_RATE_COL] = plot_df[ABSENTEEISM_RATE_COL].rolling(ROLLING_WINDOW, min_periods=1).apply(
        weighted_average, kwargs={"col": ABSENTEEISM_RATE_COL}, raw=False,
    )
    plot_df[COVID_SICK_COL] = plot_df[COVID_SICK_COL].rolling(ROLLING_WINDOW, min_periods=1).apply(
        weighted_average, kwargs={"col": COVID_SICK_COL}, raw=False,
    )
    logging.debug(f"plot_df=\n{plot_df}")

    # Filtering out holidays
    za_holidays = holidays.CountryHoliday("ZA")
    plot_df = plot_df[
        plot_df[DATE_COL_NAME].apply(lambda date: date not in za_holidays) &
        (pandas.to_datetime(plot_df[DATE_COL_NAME]).dt.weekday != 6)  # Sunday
    ]
    plot_df[DATE_COL_NAME] = pandas.to_datetime(plot_df[DATE_COL_NAME])

    return plot_df


def generate_plot(plot_df):
    start_date = plot_df[DATE_COL_NAME].max() - pandas.Timedelta(days=28)
    end_date = plot_df[DATE_COL_NAME].max() + pandas.Timedelta(days=1)
    logging.debug(f"x axis range: {start_date} - {end_date}")

    # Main plot
    line_plot = figure(
        title=None,
        plot_height=300, plot_width=None,
        x_axis_type='datetime', sizing_mode="scale_both",
        x_range=(start_date, end_date), y_axis_label="Rate (%)",
        y_range=(0, 1.05),
        toolbar_location=None,
    )

    # Adding count on the right
    line_plot.extra_y_ranges = {"count_range": Range1d(start=0, end=plot_df[DAY_COUNT_COL].max() * 1.1)}
    second_y_axis = LinearAxis(y_range_name="count_range", axis_label="Staff Assessed (#)")
    line_plot.add_layout(second_y_axis, 'right')

    # Bar plot for counts
    count_vbar = line_plot.vbar(x=DATE_COL_NAME, top=DAY_COUNT_COL, width=5e7, color="blue", source=plot_df,
                                y_range_name="count_range", alpha=0.4)

    # Line plots
    absent_line = line_plot.line(x=DATE_COL_NAME, y=ABSENTEEISM_RATE_COL, color='red', source=plot_df, line_width=5)
    absent_scatter = line_plot.scatter(x=DATE_COL_NAME, y=ABSENTEEISM_RATE_COL, fill_color='red', source=plot_df,
                                       size=12, line_alpha=0)

    covid_line = line_plot.line(x=DATE_COL_NAME, y=COVID_SICK_COL, color='orange', source=plot_df, line_width=5)
    covid_scatter = line_plot.scatter(x=DATE_COL_NAME, y=COVID_SICK_COL, fill_color='orange', source=plot_df, size=12,
                                      line_alpha=0)

    # axis formatting
    line_plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")
    line_plot.xaxis.major_label_orientation = math.pi / 4
    line_plot.axis.axis_label_text_font_size = "12pt"
    line_plot.axis.major_label_text_font_size = "12pt"

    line_plot.yaxis.formatter = NumeralTickFormatter(format="0 %")
    second_y_axis.formatter = NumeralTickFormatter(format="0 a")

    # Legend
    legend_items = [
        ("Assessed", [count_vbar]),
        ("Not at Work - 7 day mean (%)", [absent_line, absent_scatter]),
        ("Covid-19 Exposure - 7 day mean (%)", [covid_line, covid_scatter])
    ]
    legend = Legend(items=legend_items, location="center", orientation="horizontal", padding=2, margin=2)
    line_plot.add_layout(legend, "below")

    # Tooltip
    tooltips = [
        ("Date", "@Date{%F}"),
        ("Staff Absent", f"@{ABSENTEEISM_RATE_COL}{{0.0 %}}"),
        ("Covid-19 Exposure", f"@{COVID_SICK_COL}{{0.0 %}}"),
        ("Staff Assessed", f"@{DAY_COUNT_COL}{{0 a}}")
    ]
    hover_tool = HoverTool(tooltips=tooltips, mode='vline', renderers=[count_vbar], formatters={'@Date': 'datetime'})
    line_plot.add_tools(hover_tool)

    # Adding select figure below main plot
    select = figure(plot_height=75, plot_width=None, y_range=line_plot.y_range,
                    sizing_mode="scale_width",
                    x_axis_type="datetime", y_axis_type=None,
                    tools="", toolbar_location=None)
    select.extra_y_ranges = {"count_range": Range1d(start=0, end=plot_df[DAY_COUNT_COL].max() * 1.1)}
    second_y_axis = LinearAxis(y_range_name="count_range")
    select.add_layout(second_y_axis, 'right')

    range_tool = RangeTool(x_range=line_plot.x_range)
    range_tool.overlay.fill_color = "navy"
    range_tool.overlay.fill_alpha = 0.2

    select_count_line = select.line(
        y=DAY_COUNT_COL, x=DATE_COL_NAME, line_width=1, source=plot_df,
        line_color="blue", alpha=0.6, line_alpha=0.6, y_range_name="count_range"
    )
    select_absent_line = select.line(
        y=ABSENTEEISM_RATE_COL, x=DATE_COL_NAME, line_width=1, source=plot_df,
        line_color="red", alpha=0.6, line_alpha=0.6
    )
    select_covid_line = select.line(
        y=COVID_SICK_COL, x=DATE_COL_NAME, line_width=1, source=plot_df,
        line_color="orange", alpha=0.6, line_alpha=0.6
    )

    select.xgrid.grid_line_color = "White"
    select.ygrid.grid_line_color = None
    select.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")
    select.add_tools(range_tool)
    select.toolbar.active_multi = range_tool

    select.axis.axis_label_text_font_size = "12pt"
    select.axis.major_label_text_font_size = "12pt"

    combined_plot = column(line_plot, select, height_policy="max", width_policy="max")

    plot_html = file_html(combined_plot, CDN, "Business Continuity HR Capacity Time Series")

    return plot_html


def write_to_minio(data, minio_filename, minio_access, minio_secret):
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, minio_filename)

        logging.debug(f"Writing out data to '{local_path}'")
        with open(local_path, "w") as line_plot_file:
            line_plot_file.write(data)

        logging.debug(f"Uploading '{local_path}' to Minio")
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

    directorate_file_prefix = sys.argv[1]
    directorate_title = sys.argv[2]
    logging.debug(f"directorate_file_prefix={directorate_file_prefix}, directorate_title={directorate_title}")

    logging.info("Fetch[ing] data...")
    hr_transactional_data_df = get_data_df(HR_TRANSACTIONAL_FILENAME_PATH,
                                           secrets["minio"]["edge"]["access"],
                                           secrets["minio"]["edge"]["secret"])
    hr_transactional_data_df[DATE_COL_NAME] = pandas.to_datetime(hr_transactional_data_df[DATE_COL_NAME])

    hr_master_df = get_data_df(HR_MASTER_FILENAME_PATH,
                               secrets["minio"]["edge"]["access"],
                               secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")

    logging.info("Merg[ing] data...")
    hr_combined_df = merge_df(hr_transactional_data_df, hr_master_df)
    logging.info("...Merg[ed] data")

    logging.info("Filter[ing] data...")
    hr_filtered_df = directorate_filter_df(hr_combined_df, directorate_title)
    logging.info("...Filter[ing] data")

    logging.info("Add[ing] succinct status column...")
    hr_succinct_df = make_statuses_succinct_again(hr_filtered_df)
    logging.info("...Add[ed] succinct status column.")

    logging.info("Generat[ing] directorate absenteeism plot...")
    absenteeism_df = get_plot_df(hr_succinct_df)
    plot_html = generate_plot(absenteeism_df)
    plot_filename = f"{directorate_file_prefix}_{PLOT_FILENAME_SUFFIX}"
    logging.info("...Generat[ed] directorate absenteeism plot")

    plot_tuples = [(plot_html, plot_filename)]

    directorate_departments = [
        val for val in hr_filtered_df[DEPARTMENT_COL].unique()
        if pandas.notna(val)
    ] if directorate_title != "*" else []
    logging.debug(f"Departments: {', '.join(map(str, directorate_departments))}")
    for department in directorate_departments:
        logging.info(f"Generat[ing] plot for '{department}'...")
        department_file_prefix = department[:department.index("(")] if "(" in department else department
        department_file_prefix = (
            department_file_prefix.strip().lower().replace(" ", "_").replace("&", "and").replace(":", "")
        )
        logging.debug(f"department_file_prefix={department_file_prefix}")

        dept_df = directorate_filter_df(hr_filtered_df, "*", department)
        dept_succint_df = make_statuses_succinct_again(dept_df)
        absenteeism_df = get_plot_df(dept_succint_df)
        plot_html = generate_plot(absenteeism_df)

        plot_filename = f"{directorate_file_prefix}_{department_file_prefix}_{PLOT_FILENAME_SUFFIX}"

        plot_tuples += [(plot_html, plot_filename)]

        logging.info(f"...Generat[ed] plot for '{department}'")

    logging.info("Writ[ing] everything to Minio...")
    for html, filename in plot_tuples:
        write_to_minio(html, filename,
                       secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")

    logging.info("...Done!")
