import json
import math
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
from bokeh.embed import file_html
from bokeh.plotting import figure
from bokeh.layouts import column
from bokeh.models import Legend, DatetimeTickFormatter, HoverTool, RangeTool, Span
from bokeh.resources import CDN

import pandas
from tqdm.auto import tqdm

from service_delivery_latest_values_to_minio import SKIP_LIST, REFERENCE_DATE, REMAP_DICT
import service_request_timeseries_plot_widget_to_minio

tqdm.pandas()

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DIRECTORATE_COL = "directorate"
DEPARTMENT_COL = "department"

ISO601_DATE_FORMAT = "%Y-%m-%d"
DATE_COL = "Date"
DURATION_COL = "Duration"
COMPLETION_TIMESTAMP_COL = "CompletionTimestamp"
DURATION_DAYS_COL = "DurationDays"

COLS_SET = [DURATION_DAYS_COL]

DURATION_QUANTILE = 0.8
DURATION_WINDOW = 28

PREVIOUS_YEARS = 3
PREVIOUS_YEAR_TEMPLATE = "previous_{}_year"
WINDOW_START = "2020-10-01"
PLOT_START = "2019-07-01"

TOOL_TIPS = [
    (DATE_COL, f"@{DATE_COL}{{%F}}"),
    (f"Duration (vs {REFERENCE_DATE})",
     f"@{DURATION_DAYS_COL}{{0.0 a}} (+@{DURATION_DAYS_COL}_delta_relative{{0.0%}})"),
]

HOVER_COLS = [DATE_COL, DURATION_DAYS_COL]

PLOT_FILENAME_SUFFIX = "service_request_duration_plot.html"


def filter_sr_data(sr_df, directorate_name, department_name):
    logging.debug(f"Filtering directorate={directorate_name} and department={department_name}")
    return sr_df.query("(directorate == @directorate_name) & (department == @department_name)")


def generate_plot_ts_df(filtered_sr_df):
    duration_df = filtered_sr_df.groupby(
        pandas.Grouper(key=COMPLETION_TIMESTAMP_COL, freq="1D")
    )[DURATION_COL].quantile(DURATION_QUANTILE).resample("1D").ffill().ffill().to_frame()

    duration_df[DATE_COL] = duration_df.index.to_series().dt.date

    duration_df[DURATION_DAYS_COL] = (duration_df[DURATION_COL] / 3600 / 24).rolling("7D").mean()

    # Historical data
    max_date = duration_df.index.max()
    for i in range(1, PREVIOUS_YEARS + 1):
        prefix = PREVIOUS_YEAR_TEMPLATE.format(i)
        shifted_df = duration_df.shift(365 * i + 1).loc[:max_date]

        for col in COLS_SET:
            duration_df[f"{prefix}_{col}"] = shifted_df[col].copy().rolling("7D").mean()

    # Comparison with reference period
    time_filter = duration_df.index > REFERENCE_DATE
    for col in COLS_SET:
        duration_df.loc[time_filter, f"{col}_delta"] = duration_df[col] - duration_df.loc[REFERENCE_DATE, col]
        duration_df.loc[time_filter, f"{col}_delta_relative"] = (
                duration_df[f"{col}_delta"] / duration_df.loc[REFERENCE_DATE, col]
        )

    duration_df = duration_df.loc[PLOT_START:]

    logging.debug(f"duration_df.shape={duration_df.shape}")
    logging.debug(f"duration_df.columns={duration_df.columns}")
    logging.debug(f"duration_df.tail(10)=\n{duration_df.tail(10)}")

    return duration_df


def generate_plot(plot_df):
    window_start = pandas.to_datetime(WINDOW_START, format=ISO601_DATE_FORMAT)
    window_end = plot_df.index.max() + pandas.Timedelta(days=1)

    y_range_end = plot_df.loc[WINDOW_START:, DURATION_DAYS_COL].max() * 1.1

    logging.debug(f"x axis range: {window_start} - {window_end}")

    plot = figure(
        plot_height=None, plot_width=None,
        sizing_mode="scale_both",
        x_range=(window_start, window_end),
        y_range=(0, y_range_end),
        x_axis_type='datetime', toolbar_location=None,
        y_axis_label="Service Request Duration (Days)",
        background_fill_color="#eaeaf2",
    )

    # Previous year lines
    for i in range(1, PREVIOUS_YEARS + 1):
        prefix = PREVIOUS_YEAR_TEMPLATE.format(i)
        for col in COLS_SET:
            previous_year_line = plot.line(x=DATE_COL, y=f"{prefix}_{col}",
                                           source=plot_df, color="Grey", line_width=2, alpha=0.5)

    line = plot.line(x=DATE_COL, y=DURATION_DAYS_COL,
                     source=plot_df, color="#c44e52", line_width=4, alpha=0.8, line_alpha=0.8)
    circle = plot.circle(x=DATE_COL, y=DURATION_DAYS_COL,
                         source=plot_df, color="#c44e52", size=6, alpha=0.8, line_alpha=0.8)

    # Marker line
    marker_span = Span(
        location=pandas.to_datetime(REFERENCE_DATE),
        dimension='height', line_color="#4c72b0",
        line_dash='dashed', line_width=4
    )
    plot.add_layout(marker_span)

    # Plot grid and axis
    plot.grid.grid_line_color = "white"
    plot.xaxis.major_label_orientation = math.pi / 4
    plot.xaxis.formatter = DatetimeTickFormatter(days=ISO601_DATE_FORMAT)
    plot.y_range.start = 0

    legend_items = [("Duration (days)", [line, circle]),
                    ("Previous Years", [previous_year_line])]

    legend = Legend(items=legend_items, location="center", orientation="horizontal", margin=2, padding=2)
    plot.add_layout(legend, 'below')

    hover_tool = HoverTool(tooltips=TOOL_TIPS, renderers=[line], mode="vline",
                           formatters={f'@{DATE_COL}': 'datetime'})
    plot.add_tools(hover_tool)

    # Adding select figure below main plot
    select = figure(plot_height=75, plot_width=None, y_range=plot.y_range,
                    sizing_mode="scale_width",
                    x_axis_type="datetime", y_axis_type=None,
                    tools="", toolbar_location=None, background_fill_color="#eaeaf2")

    range_tool = RangeTool(x_range=plot.x_range)
    range_tool.overlay.fill_color = "navy"
    range_tool.overlay.fill_alpha = 0.2

    select_line = select.line(
        y=DURATION_DAYS_COL, x=DATE_COL, line_width=1, source=plot_df,
        line_color="#c44e52", alpha=0.6, line_alpha=0.6
    )

    select.xgrid.grid_line_color = "White"
    select.ygrid.grid_line_color = None
    select.add_tools(range_tool)
    select.toolbar.active_multi = range_tool

    combined_plot = column(plot, select, height_policy="max", width_policy="max")

    plot_html = file_html(combined_plot, CDN, "Business Continuity Service Request Duration Time Series")

    return plot_html


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("Fetch[ing] SR data...")
    service_request_df = service_request_timeseries_plot_widget_to_minio.get_service_request_data(
        service_request_timeseries_plot_widget_to_minio.DATA_BUCKET_NAME,
        secrets["minio"]["lake"]["access"],
        secrets["minio"]["lake"]["secret"]
    )
    logging.info("...Fetch[ed] SR data.")
    logging.debug(f"service_request_df.shape={service_request_df.shape}")

    org_set = service_request_df[[DIRECTORATE_COL, DEPARTMENT_COL]].reset_index(drop=True).drop_duplicates()
    for directorate_title, department_title in ([(None, None)] + list(org_set.values)):
        directorate = directorate_title.strip().lower().replace(" ", "_") if directorate_title else None
        directorate = REMAP_DICT.get(directorate, directorate)
        department = department_title.strip().lower().replace(" ", "_") if department_title else None

        if directorate in SKIP_LIST or department in SKIP_LIST:
            logging.warning(f"Skipping {directorate}_{department}!")
            continue

        logging.info(f"Filter[ing] data for '{directorate}'_'{department}'...")
        filter_sr_df = (filter_sr_data(service_request_df, directorate_title, department_title)
                        if directorate_title and department_title else
                        service_request_df)
        logging.info(f"...Filter[ed] data")
        logging.debug(f"directorate_df.shape={filter_sr_df.shape}")

        logging.info("Mung[ing] data for plotting...")
        ts_plot_df = generate_plot_ts_df(filter_sr_df)
        logging.info("...Mung[ed] data")

        logging.info("Generat[ing] Plot...")
        ts_plot_html = generate_plot(ts_plot_df)
        logging.info("...Generat[ed] Plot")

        logging.info("Writ[ing] to Minio...")
        plot_prefix = "city"
        plot_prefix = f"{plot_prefix}_{directorate}_{department}" if directorate and department else plot_prefix

        plot_filename = f"{plot_prefix}_{PLOT_FILENAME_SUFFIX}"
        service_request_timeseries_plot_widget_to_minio.write_to_minio(ts_plot_html, plot_filename,
                                                                       secrets["minio"]["edge"]["access"],
                                                                       secrets["minio"]["edge"]["secret"])
        logging.info("...Wr[ote] to Minio")
