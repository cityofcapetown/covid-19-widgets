import json
import logging
import os
import sys

from bokeh.embed import file_html
from bokeh.layouts import column
from bokeh.models import Legend, HoverTool, RangeTool, Span, Range1d, LinearAxis, NumeralTickFormatter
from bokeh.plotting import figure
from bokeh.resources import CDN

import pandas

import service_request_timeseries_plot_widget_to_minio
from service_delivery_latest_values_to_minio import DATE_COL, FEATURE_COL, MEASURE_COL, VALUE_COL, SKIP_LIST, REMAP_DICT
import service_delivery_latest_values_to_minio
import service_delivery_volume_code_metric_plot_widgets_to_minio

STILL_OPEN_COL = "opened_still_open_sum"
OPENED_COL = "opened_count"

MEASURES_SET = {STILL_OPEN_COL, OPENED_COL}

AGE_COL = "age"
OPEN_PROPORTION_COL = "open_proportion"
CUMULATIVE_PROPORTION_COL = "open_cumulative_proportion"

HOVER_COLS = [
    ("Age", f"@{AGE_COL}{{0 a}} days"),
    ("Date Opened", f"@{DATE_COL}{{%F}}"),
    ("Requests Open", f"@{STILL_OPEN_COL}{{0 a}}"),
    ("Cumulative Open Proportion", f"@{CUMULATIVE_PROPORTION_COL}{{0.[00]%}}")
]

DEFAULT_WINDOW_SIZE = 270
MARKER_LOCATION = 180

PLOT_FILENAME_SUFFIX = "service_delivery_request_distribution_plot.html"


def generate_plot_data(data_df):
    pivot_df = data_df.pivot(index=DATE_COL, columns=MEASURE_COL, values=VALUE_COL)

    still_open_df = pivot_df[[STILL_OPEN_COL]].copy()

    # Age Calculation
    max_date = still_open_df.index.max()
    still_open_df[AGE_COL] = (max_date - still_open_df.index).days

    # Proportional Calculations
    open_sum = pivot_df[OPENED_COL].sum()
    still_open_df[OPEN_PROPORTION_COL] = pivot_df[STILL_OPEN_COL] / open_sum
    still_open_df[CUMULATIVE_PROPORTION_COL] = still_open_df[OPEN_PROPORTION_COL].cumsum()

    return still_open_df


def generate_plot(plot_df):
    window_start = plot_df.query(f"{AGE_COL} >= {DEFAULT_WINDOW_SIZE}").index.max()

    # Creating Main Plot
    window_total_max = plot_df.loc[plot_df.index > window_start,
                                   [STILL_OPEN_COL]].max().max() * 1.1
    logging.debug(f"window_start={window_start}, window_total_max={window_total_max}")

    plot = figure(
        plot_height=None, plot_width=None,
        sizing_mode="scale_both",
        x_range=(-5, DEFAULT_WINDOW_SIZE),
        y_range=(0, window_total_max),
        toolbar_location=None,
        x_axis_label="Request Age (days)",
        y_axis_label="Open SR Count (#)",
        background_fill_color="#eaeaf2",
    )
    plot.extra_y_ranges = {"proportion": Range1d(start=0, end=plot_df[CUMULATIVE_PROPORTION_COL].max() * 1.1)}
    secondary_axis = LinearAxis(y_range_name="proportion", axis_label="Request Proportion (%)")
    plot.add_layout(secondary_axis, 'right')

    # Open and less than 180 days
    vbar_open = plot.vbar(
        top=STILL_OPEN_COL, x=AGE_COL, width=0.75, source=plot_df.query(f"{AGE_COL} < @MARKER_LOCATION"),
        fill_color="#4c72b0", line_color="#4c72b0", alpha=0.8, line_alpha=0.8
    )
    # Open and more than 180 days
    vbar_still_open = plot.vbar(
        top=STILL_OPEN_COL, x=AGE_COL, width=0.75, source=plot_df.query(f"{AGE_COL} >= @MARKER_LOCATION"),
        fill_color="#c44e52", line_color="#c44e52", alpha=0.8, line_alpha=0.8
    )

    # 180 day Marker line
    marker_span = Span(
        location=MARKER_LOCATION,
        dimension='height', line_color="#dd8452",
        line_dash='dashed', line_width=4
    )
    plot.add_layout(marker_span)

    # Cumulative Proportion Line
    cum_prop_line = plot.line(
        y=CUMULATIVE_PROPORTION_COL, x=AGE_COL, width=2, source=plot_df, y_range_name="proportion",
        line_color="#55a868", alpha=0.8, line_alpha=0.8
    )

    # Plot grid and axis
    service_delivery_volume_code_metric_plot_widgets_to_minio.stlye_axes(plot)
    plot.yaxis.formatter = NumeralTickFormatter(format="0 a")
    secondary_axis.formatter = NumeralTickFormatter(format="0.[00] %")

    # Plot legend
    legend_items = [("Open < 180 days", [vbar_open]),
                    ("Open > 180 days", [vbar_still_open]),
                    ("Open Requests", [cum_prop_line])]
    service_delivery_volume_code_metric_plot_widgets_to_minio.add_legend(plot, legend_items)

    # Plot tooltip
    hover_tool = HoverTool(tooltips=HOVER_COLS, mode="vline",
                           formatters={f'@{DATE_COL}': 'datetime'},
                           renderers=[cum_prop_line])
    plot.add_tools(hover_tool)

    # Adding select figure below main plot
    select = figure(plot_height=75, plot_width=None,
                    y_range=plot.y_range,
                    sizing_mode="scale_width",
                    y_axis_type=None,
                    tools="", toolbar_location=None, background_fill_color="#eaeaf2")

    range_tool = RangeTool(x_range=plot.x_range)
    range_tool.overlay.fill_color = "navy"
    range_tool.overlay.fill_alpha = 0.2

    select_open = select.line(
        y=STILL_OPEN_COL, x=AGE_COL, line_width=1, source=plot_df.query(f"{AGE_COL} < @MARKER_LOCATION"),
        line_color="#4c72b0", alpha=0.6, line_alpha=0.6
    )
    select_still_open = select.line(
        y=STILL_OPEN_COL, x=AGE_COL, line_width=1, source=plot_df.query(f"{AGE_COL} >= @MARKER_LOCATION"),
        line_color="#c44e52", alpha=0.6, line_alpha=0.6
    )

    select.xgrid.grid_line_color = "White"
    service_delivery_volume_code_metric_plot_widgets_to_minio.stlye_axes(select)

    select.ygrid.grid_line_color = None

    select.add_tools(range_tool)
    select.toolbar.active_multi = range_tool

    combined_plot = column(plot, select, height_policy="max", width_policy="max")

    plot_html = file_html(combined_plot, CDN, "Business Continuity Service Delivery Request Age Distribution")

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
        feature_clauses = [REMAP_DICT.get(feature_val, feature_val) for feature_val in feature_clauses]

        city_file_prefix = feature_clauses[0]
        directorate_file_prefix = feature_clauses[1] if len(feature_clauses) > 1 else None
        department_file_prefix = feature_clauses[2] if len(feature_clauses) > 2 else None

        if ((directorate_file_prefix and directorate_file_prefix in SKIP_LIST) or
                (department_file_prefix and department_file_prefix in SKIP_LIST)):
            logging.warning(f"skipping {feature}!")
            continue

        logging.info(f"Generat[ing] plot for '{feature}'")

        logging.debug(f"service_request_df.shape={sd_timeseries_df.shape}")
        logging.info(f"Filter[ing] data...")
        filtered_df = sd_timeseries_df.query(
            f"(feature == @feature) and measure.isin(@MEASURES_SET)"
        )
        logging.info(f"...Filter[ed] data")
        logging.debug(f"directorate_df.shape={filtered_df.shape}")

        logging.info("Mung[ing] data for plotting...")
        dist_plot_df = generate_plot_data(filtered_df)
        logging.info("...Mung[ed] data")

        logging.info("Generat[ing] Plot...")
        plot_html = generate_plot(dist_plot_df)
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
