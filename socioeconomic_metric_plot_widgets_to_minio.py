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
from bokeh.palettes import d3

import pandas

import service_request_timeseries_utils
from socioeconomic_latest_values_to_minio import DATE_COL, FEATURE_COL, MEASURE_COL, VALUE_COL
import socioeconomic_latest_values_to_minio

TS_MEASURES = (
    "GDP Growth", "Poverty", "Unemployment"
)

PLOT_SUFFIX = "plot.html"


def generate_plot_timeseries(data_df):
    pivot_df = data_df.pivot(
        index=[DATE_COL],
        columns=FEATURE_COL, values=VALUE_COL
    ).reset_index()

    logging.debug(f"pivot_df.shape={pivot_df.shape}")
    logging.debug(f"pivot_df.head(10)=\n{pivot_df.head(10)}")

    return pivot_df


def generate_ts_plot(plot_df, measure_label, features):
    window_max = plot_df[features].max().max()*1.1
    window_min = plot_df[features].min().min()*1.1
    window_abs = max([abs(window_max), abs(window_min)])

    window_max = 0 if window_max < 0 else window_abs
    window_min = 0 if window_min > 0 else -window_abs

    plot = figure(
        plot_height=None, plot_width=None,
        sizing_mode="scale_both",
        y_range=(window_min, window_max),
        x_axis_type='datetime', toolbar_location=None,
        y_axis_label=f"{measure_label} (%)",
    )

    # Metric lines
    palette = d3['Category10'][max([len(features), 3])]
    for i, feature in enumerate(features):
        metric_line = plot.line(
            y=feature, x=DATE_COL, width=4, source=plot_df,
            alpha=0.8, line_alpha=0.8, line_color=palette[i],
            line_dash='dashed' if "projection" in feature.lower() else 'solid',
            legend_label=feature, muted_alpha=0.2,
        )
        metric_circle = plot.circle(x=DATE_COL, y=feature, color=palette[i], legend_label=feature,
                                    source=plot_df, size=6, alpha=0.8, line_alpha=0.8, muted_alpha=0.2,)

        # line tooltips
        tooltips = [
            (DATE_COL, f"@{DATE_COL}{{%F}}"),
            (feature, f"@{{{feature}}}{{0.[0] %}}")
        ]
        hover_tool = HoverTool(tooltips=tooltips, renderers=[metric_line],
                               formatters={f'@{DATE_COL}': 'datetime'})
        plot.add_tools(hover_tool)

    # Plot grid and axis
    plot.grid.grid_line_color = "white"
    plot.xaxis.major_label_orientation = math.pi / 4
    # plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")
    plot.yaxis.formatter = NumeralTickFormatter(format="+0.0%")

    plot.axis.axis_label_text_font_size = "12pt"
    plot.axis.major_label_text_font_size = "12pt"

    # Plot legend
    plot.legend.click_policy = 'mute'
    plot.legend.location = "bottom_center"

    plot_html = file_html(plot, CDN, f"Socioeconomic {measure} Time Series")

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

    logging.info("Fetch[ing] SE data...")
    # Getting service delivery time series
    se_timeseries_df = socioeconomic_latest_values_to_minio.get_data(
        socioeconomic_latest_values_to_minio.SE_DATA_FILENAME,
        secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"]
    )
    se_timeseries_df[DATE_COL] = pandas.to_datetime(se_timeseries_df[DATE_COL],
                                                    format=socioeconomic_latest_values_to_minio.DATE_FORMAT)
    logging.info("...Fetch[ed] SD data.")

    measures = se_timeseries_df[FEATURE_COL].unique()
    for measure in TS_MEASURES:
        logging.info(f"Generat[ing] plot for '{measure}'")

        logging.info("Filter[ing] data...")
        filtered_df = se_timeseries_df.query(f"{MEASURE_COL} == @measure")
        logging.info("...Filter[ed] data")

        logging.info("Mung[ing] data for plotting...")
        metric_plot_df = generate_plot_timeseries(filtered_df)
        logging.info("...Mung[ed] data")

        logging.info(f"Generat[ing] Plot for {measure}...")
        plot_html = generate_ts_plot(metric_plot_df, measure, filtered_df[FEATURE_COL].unique())
        logging.info(f"...Generat[ed] {measure} Plot")

        logging.info("Writ[ing] to Minio...")
        measure_prefix = measure.lower().replace(" ", "_")
        plot_filename = f"{socioeconomic_latest_values_to_minio.SE_PREFIX}_{measure_prefix}_ts_{PLOT_SUFFIX}"

        service_request_timeseries_utils.write_to_minio(plot_html, plot_filename,
                                                        secrets["minio"]["edge"]["access"],
                                                        secrets["minio"]["edge"]["secret"])
        logging.info("...Wr[ote] to Minio")
