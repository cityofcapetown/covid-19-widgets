import json
import math
import logging
import os
import sys

from bokeh.embed import file_html
from bokeh.layouts import column
from bokeh.models import Legend, DatetimeTickFormatter, HoverTool, RangeTool, NumeralTickFormatter, Span
from bokeh.plotting import figure
from bokeh.resources import CDN

import pandas

import service_request_timeseries_plot_widget_to_minio
from service_delivery_latest_values_to_minio import DATE_COL, FEATURE_COL, MEASURE_COL, VALUE_COL, REFERENCE_DATE, SKIP_LIST, REMAP_DICT
import service_delivery_latest_values_to_minio

BACKLOG = "backlog"
SERVICE_STANDARD = "service_standard"
LONG_BACKLOG = "long_backlog"
COLS_SET = {BACKLOG, SERVICE_STANDARD, LONG_BACKLOG}

PREVIOUS_YEARS = 3
PREVIOUS_YEAR_TEMPLATE = "previous_{}_year"

# metric_label, unit_label, line_colour, marker_line_colour
# colours from default Seaborn palette: https://seaborn.pydata.org/generated/seaborn.color_palette.html
COL_PLOT_SETTINGS = {
    BACKLOG: ("Backlog", "# of requests", "#4c72b0", "#c44e52"),
    SERVICE_STANDARD: ("Service Standard", "%", "#c44e52", "#4c72b0"),
    LONG_BACKLOG: ("Still Open > 180 days", "%", "#55a868", "#dd8452")
}

TOOL_TIPS = [
    (DATE_COL, f"@{DATE_COL}{{%F}}"),
    (f"Backlog (vs {REFERENCE_DATE})", f"@{BACKLOG}{{0.0 a}} (@{BACKLOG}_delta_relative{{+0.0%}})"),
    (f"Service Standard (vs {REFERENCE_DATE})", f"@{SERVICE_STANDARD}{{0.0%}} (@{SERVICE_STANDARD}_delta{{+0.0%}})"),
    (f"Still Open > 180 days (vs {REFERENCE_DATE})", f"@{LONG_BACKLOG}{{0.0%}} (@{LONG_BACKLOG}_delta{{+0.0%}})"),
]
AXIS_FORMATTERS = {
    BACKLOG: '0 a',
    SERVICE_STANDARD: '0 %',
    LONG_BACKLOG: '0 %'
}

DEFAULT_WINDOW_SIZE = 28
PLOT_START = "2019-09-01"

WINDOW_START = "2020-10-01"
REFERENCE_DATE = "2020-10-12"

SD_PREFIX = "service_delivery"
PLOT_SUFFIX = "plot.html"


def generate_plot_timeseries(data_df):
    pivot_df = data_df.pivot(
        index=[DATE_COL, FEATURE_COL],
        columns=MEASURE_COL, values=VALUE_COL
    ).reset_index()

    resampled_df = pivot_df.set_index(DATE_COL).resample("1D").ffill()

    # Historical data
    max_date = resampled_df.index.max()
    for i in range(1, PREVIOUS_YEARS + 1):
        prefix = PREVIOUS_YEAR_TEMPLATE.format(i)
        shifted_df = resampled_df.shift(365 * i + 1).loc[:max_date]

        for col in COLS_SET:
            resampled_df[f"{prefix}_{col}"] = shifted_df[col].copy().rolling(7).median()

    # Comparison with reference period
    time_filter = resampled_df.index > REFERENCE_DATE
    for col in COLS_SET:
        resampled_df.loc[time_filter, f"{col}_delta"] = resampled_df[col] - resampled_df.loc[REFERENCE_DATE, col]
        resampled_df.loc[time_filter, f"{col}_delta_relative"] = (
                resampled_df[f"{col}_delta"] / resampled_df.loc[REFERENCE_DATE, col]
        )

    return resampled_df.loc[PLOT_START:]


def generate_plot(plot_df, metric_col,
                  metric_label="Backlog", unit_label="# of requests",
                  line_colour="#4c72b0", marker_line="#c44e52"):
    # Creating Main Plot
    window_start = plot_df.loc[WINDOW_START:].index.min()
    window_end = plot_df.index.max() + pandas.Timedelta(days=1)

    window_total_max = plot_df.loc[plot_df.index > window_start,
                                   [metric_col, ]].max().max() * 1.1
    plot_df_min = plot_df.loc[:,
                  [metric_col, ]].min().min()
    window_total_min = plot_df_min * (1.1 if plot_df_min < 0 else 0.9)
    # window_total = max(abs(window_total_max), abs(window_total_min))

    plot = figure(
        plot_height=300, plot_width=None,
        sizing_mode="scale_both",
        x_range=(window_start, window_end),
        y_range=(window_total_min, window_total_max),
        x_axis_type='datetime', toolbar_location=None,
        y_axis_label=f"{metric_label} ({unit_label})",
        background_fill_color="#eaeaf2",
    )

    # Previous year lines
    for i in range(1, PREVIOUS_YEARS + 1):
        prefix = PREVIOUS_YEAR_TEMPLATE.format(i)
        previous_year_line = plot.line(x=DATE_COL, y=f"{prefix}_{metric_col}",
                                       source=plot_df, color="Grey", line_width=2, alpha=0.5)

    # Metric line
    metric_line = plot.line(
        y=metric_col, x=DATE_COL, width=4, source=plot_df,
        line_color=line_colour, alpha=0.8, line_alpha=0.8
    )
    metric_circle = plot.circle(x=DATE_COL, y=metric_col,
                                source=plot_df, color=line_colour, size=6, alpha=0.8, line_alpha=0.8)

    # Marker line
    marker_span = Span(
        location=pandas.to_datetime(REFERENCE_DATE),
        dimension='height', line_color=marker_line,
        line_dash='dashed', line_width=4
    )
    plot.add_layout(marker_span)

    # Plot grid and axis
    plot.grid.grid_line_color = "white"
    plot.xaxis.major_label_orientation = math.pi / 4
    plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")
    plot.yaxis.formatter = NumeralTickFormatter(format=AXIS_FORMATTERS[metric_col])

    plot.axis.axis_label_text_font_size = "12pt"
    plot.axis.major_label_text_font_size = "12pt"

    # Plot legend
    legend_items = [(metric_label, [metric_line, metric_circle]),
                    ("Previous Years", [previous_year_line])]
    legend = Legend(items=legend_items, location="center", orientation="horizontal", margin=2, padding=2)
    plot.add_layout(legend, 'below')

    # Plot tooltip
    tooltips = TOOL_TIPS
    hover_tool = HoverTool(tooltips=tooltips, renderers=[metric_line], mode="vline",
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
        y=metric_col, x=DATE_COL, line_width=1, source=plot_df,
        line_color=line_colour, alpha=0.6, line_alpha=0.6
    )

    select_span = Span(
        location=pandas.to_datetime(REFERENCE_DATE),
        dimension='height', line_color=marker_line,
        line_dash='dashed', line_width=1
    )
    select.add_layout(select_span)

    select.xgrid.grid_line_color = "White"
    select.ygrid.grid_line_color = None
    select.add_tools(range_tool)
    select.toolbar.active_multi = range_tool

    select.axis.axis_label_text_font_size = "12pt"
    select.axis.major_label_text_font_size = "12pt"

    combined_plot = column(plot, select, height_policy="max", width_policy="max")

    plot_html = file_html(combined_plot, CDN, f"Business Continuity Service Delivery {metric_label} Time Series")

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
            f"(feature == @feature) and measure.isin(@COLS_SET)"
        )
        logging.info(f"...Filter[ed] data")
        logging.debug(f"filtered_df.shape={filtered_df.shape}")

        logging.info("Mung[ing] data for plotting...")
        metric_plot_df = generate_plot_timeseries(filtered_df)
        logging.info("...Mung[ed] data")

        for metric_col in COLS_SET:
            if metric_col not in filtered_df[MEASURE_COL].unique():
                logging.warning(f"skipping '{feature}' for metric '{metric_col}' - it's not present!")
                continue

            metric_label, unit_label, line_colour, marker_line_colour = COL_PLOT_SETTINGS[metric_col]

            logging.info(f"Generat[ing] Plot for {metric_col}...")
            plot_html = generate_plot(metric_plot_df, metric_col,
                                      metric_label, unit_label, line_colour, marker_line_colour)
            logging.info(f"...Generat[ed] {metric_col} Plot")

            logging.info("Writ[ing] to Minio...")
            plot_filename_prefix = city_file_prefix
            plot_filename_prefix = (f"{plot_filename_prefix}_{directorate_file_prefix}"
                                    if directorate_file_prefix else plot_filename_prefix)
            plot_filename_prefix = (f"{plot_filename_prefix}_{department_file_prefix}"
                                    if department_file_prefix else plot_filename_prefix)

            plot_filename = f"{SD_PREFIX}_{plot_filename_prefix}_{metric_col}_{PLOT_SUFFIX}"
            service_request_timeseries_plot_widget_to_minio.write_to_minio(plot_html, plot_filename,
                                                                           secrets["minio"]["edge"]["access"],
                                                                           secrets["minio"]["edge"]["secret"])
            logging.info("...Wr[ote] to Minio")
