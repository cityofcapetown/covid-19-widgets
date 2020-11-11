import json
import math
import logging
import os
import sys

from bokeh.embed import file_html
from bokeh.layouts import column
from bokeh.models import Legend, DatetimeTickFormatter, HoverTool, RangeTool
from bokeh.plotting import figure
from bokeh.resources import CDN

import pandas

import service_request_timeseries_plot_widget_to_minio
from service_delivery_latest_values_to_minio import DATE_COL, FEATURE_COL, MEASURE_COL, VALUE_COL
import service_delivery_latest_values_to_minio

OPENED_COUNT_COL = "opened_count"
CLOSED_COUNT_COL = "closed_count"
COLS_SET = {OPENED_COUNT_COL, CLOSED_COUNT_COL}

PREVIOUS_YEARS = 3
PREVIOUS_YEAR_TEMPLATE = "previous_{}_year"
PLOT_START = "2019-07-01"

HOVER_COLS = [OPENED_COUNT_COL, CLOSED_COUNT_COL, DATE_COL]
DEFAULT_WINDOW_SIZE = 28

PLOT_FILENAME_SUFFIX = "service_delivery_volume_plot.html"


def generate_plot_timeseries(data_df):
    pivot_df = data_df.pivot(
        index=[DATE_COL, FEATURE_COL],
        columns=MEASURE_COL, values=VALUE_COL
    ).reset_index()

    resampled_df = pivot_df.set_index(DATE_COL).resample("1D").sum()
    resampled_df[CLOSED_COUNT_COL] = resampled_df[CLOSED_COUNT_COL]*-1

    max_date = resampled_df.index.max()
    for i in range(1, PREVIOUS_YEARS + 1):
        prefix = PREVIOUS_YEAR_TEMPLATE.format(i)
        shifted_df = resampled_df.shift(365*i + 1).loc[:max_date]

        for col in COLS_SET:
            resampled_df[f"{prefix}_{col}"] = shifted_df[col].copy().rolling(7).median()

    return resampled_df.loc[PLOT_START:]


def generate_plot(plot_df):
    # Creating Main Plot
    window_start = plot_df.index.max() - pandas.Timedelta(days=DEFAULT_WINDOW_SIZE)
    window_end = plot_df.index.max() + pandas.Timedelta(days=1)

    window_total_max = plot_df.loc[plot_df.index > window_start,
                                   [OPENED_COUNT_COL, ]].max().max() * 1.1
    window_total_min = plot_df.loc[plot_df.index > window_start,
                                   [CLOSED_COUNT_COL, ]].min().min() * 1.1
    window_total = max(abs(window_total_max), abs(window_total_min))

    plot = figure(
        plot_height=None, plot_width=None,
        sizing_mode="scale_both",
        x_range=(window_start, window_end),
        y_range=(-window_total, window_total),
        x_axis_type='datetime', toolbar_location=None,
        y_axis_label="SR Count (#)",
        background_fill_color="#eaeaf2",
    )

    # Previous year lines
    for i in range(1, PREVIOUS_YEARS + 1):
        prefix = PREVIOUS_YEAR_TEMPLATE.format(i)
        for col in COLS_SET:
            previous_year_line = plot.line(x=DATE_COL, y=f"{prefix}_{col}",
                                           source=plot_df, color="Grey", line_width=2, alpha=0.5)

    # Opened and Count bar plot
    vbar_opened = plot.vbar(
        top=OPENED_COUNT_COL, x=DATE_COL, width=pandas.Timedelta(days=0.75), source=plot_df,
        fill_color="#4c72b0", line_color="#4c72b0", alpha=0.8, line_alpha=0.8
    )
    vbar_closed = plot.vbar(
        top=CLOSED_COUNT_COL, x=DATE_COL, width=pandas.Timedelta(days=0.75), source=plot_df,
        fill_color="#c44e52", line_color="#c44e52", alpha=0.8, line_alpha=0.8
    )

    # Plot grid and axis
    plot.grid.grid_line_color = "white"
    plot.xaxis.major_label_orientation = math.pi / 4
    plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")

    # Plot legend
    legend_items = [("Opened", [vbar_opened]),
                    ("Closed", [vbar_closed]),
                    ("Previous Years", [previous_year_line])]
    legend = Legend(items=legend_items, location="center", orientation="horizontal", margin=2, padding=2)
    plot.add_layout(legend, 'below')

    # Plot tooltip
    tooltips = [
        (DATE_COL, f"@{DATE_COL}{{%F}}"),
        *[(col, f"@{col}{{0.0 a}}") for col in HOVER_COLS if col != DATE_COL]
    ]
    hover_tool = HoverTool(tooltips=tooltips, renderers=[vbar_opened], mode="vline",
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

    select_opened = select.line(
        y=OPENED_COUNT_COL, x=DATE_COL, line_width=1, source=plot_df,
        line_color="#4c72b0", alpha=0.6, line_alpha=0.6
    )
    select_closed = select.line(
        y=CLOSED_COUNT_COL, x=DATE_COL, line_width=1, source=plot_df,
        line_color="#c44e52", alpha=0.6, line_alpha=0.6
    )

    select.xgrid.grid_line_color = "White"
    select.ygrid.grid_line_color = None
    select.add_tools(range_tool)
    select.toolbar.active_multi = range_tool

    combined_plot = column(plot, select, height_policy="max", width_policy="max")

    plot_html = file_html(combined_plot, CDN, "Business Continuity Service Delivery Volume Time Series")

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

    logging.info("Fetch[ing] SD data...")
    # Getting service delivery time series
    sd_timeseries_df = service_delivery_latest_values_to_minio.get_data(
        service_delivery_latest_values_to_minio.SD_DATA_FILENAME,
        secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"]
    )
    sd_timeseries_df[DATE_COL] = pandas.to_datetime(sd_timeseries_df[DATE_COL])
    logging.info("...Fetch[ed] SD data.")

    features = sd_timeseries_df[FEATURE_COL].unique()
    for feature in features:
        feature_clauses = feature.split("-")
        city_file_prefix = feature_clauses[0]
        directorate_file_prefix = feature_clauses[1] if len(feature_clauses) > 1 else None
        department_file_prefix = feature_clauses[2] if len(feature_clauses) > 2 else None

        logging.info(f"Generat[ing] plot for '{feature}'")

        logging.debug(f"service_request_df.shape={sd_timeseries_df.shape}")
        logging.info(f"Filter[ing] data...")
        filtered_df = sd_timeseries_df.query(
            f"(feature == @feature) and measure.isin(@COLS_SET)"
        )
        logging.info(f"...Filter[ed] data")
        logging.debug(f"directorate_df.shape={filtered_df.shape}")

        logging.info("Mung[ing] data for plotting...")
        volume_plot_df = generate_plot_timeseries(filtered_df)
        logging.info("...Mung[ed] data")

        logging.info("Generat[ing] Plot...")
        plot_html = generate_plot(volume_plot_df)
        logging.info("...Generat[ed] Plot")

        logging.info("Writ[ing] to Minio...")
        plot_filename_prefix = city_file_prefix
        plot_filename_prefix = (f"{plot_filename_prefix}_{directorate_file_prefix}"
                                if directorate_file_prefix else plot_filename_prefix)
        plot_filename_prefix = (f"{plot_filename_prefix}_{department_file_prefix}"
                                if department_file_prefix else plot_filename_prefix)

        plot_filename = f"{plot_filename_prefix}_{PLOT_FILENAME_SUFFIX}"
        service_request_timeseries_plot_widget_to_minio.write_to_minio(plot_html, plot_filename,
                                                                       secrets["minio"]["edge"]["access"],
                                                                       secrets["minio"]["edge"]["secret"])
        logging.info("...Wr[ote] to Minio")
