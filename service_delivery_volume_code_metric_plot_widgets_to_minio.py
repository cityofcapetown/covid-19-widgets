import json
import logging
import os
import sys

from bokeh.embed import file_html
from bokeh.models import Legend, HoverTool, NumeralTickFormatter
from bokeh.plotting import figure
from bokeh.resources import CDN

from service_delivery_latest_values_to_minio import REFERENCE_DATE
import service_delivery_latest_values_to_minio
import service_request_timeseries_plot_widget_to_minio

SD_DEPT_METRICS = "business_continuity_service_delivery_department_metrics.csv"

SD_PREFIX = "service_delivery"
VOLUME_PERF_PLOT_SUFFIX = "volume_perf_plot.html"
VOLUME_GROWTH_PLOT_SUFFIX = "volume_growth_plot.html"

CITY_VALUE = (None, None)
DEPARTMENTS = [
    ("Energy and Climate Change", "Electricity"),
    ("Water and Waste Services", "Water and Sanitation"),
    ("Water and Waste Services", "Solid Waste Management"),
    ("Transport", "Roads Infrastructure and Management"),
    ("Human Settlements", "Public Housing"),
    ("Community Services and Health", "Recreation and Parks")
]

DIRECTORATE_COL = "directorate"
DEPARTMENT_COL = "department"
CODE_COL = "Code"
OPEN_COUNT_COL = "opened_count"
OPEN_WITHIN_TARGET_COL = "opened_within_target_sum"
CLOSED_COUNT_COL = "closed_count"

OUTSIDE_TARGET_COL = "opened_without_target_sum"
GROWTH_COL = "growth"
GROWTH_COLOUR_COL = "growth_colour"

TOOL_TIPS = [
    ("Directorate", f"@{DIRECTORATE_COL}"),
    ("Department", f"@{DEPARTMENT_COL}"),
    ("Request Type", f"@{CODE_COL}"),
    (f"Opened since {REFERENCE_DATE} (Within/Outside Target)",
     f"@{OPEN_COUNT_COL}{{0.[0] a}} (@{OPEN_WITHIN_TARGET_COL}{{0.[0] a}} / @{OUTSIDE_TARGET_COL}{{0.[0] a}})"),
    (f"Closed since {REFERENCE_DATE}", f"@{CLOSED_COUNT_COL}{{0.[0] a}}"),
    (f"Nett Growth since {REFERENCE_DATE}", f"@{GROWTH_COL}{{0.[0] a}}"),
]

PLOT_CODES = 10
GOOD_COLOUR = "#4c72b0"
BAD_COLOUR = "#c44e52"
BACKGROUND_COLOUR = "#eaeaf2"


def generate_plot_df(data_df):
    plot_df = data_df.copy().sort_values(by=OPEN_COUNT_COL, ascending=False)

    plot_df[OUTSIDE_TARGET_COL] = plot_df[OPEN_COUNT_COL] - plot_df[OPEN_WITHIN_TARGET_COL]
    plot_df[GROWTH_COL] = plot_df[OPEN_COUNT_COL] - plot_df[CLOSED_COUNT_COL]
    plot_df[GROWTH_COLOUR_COL] = plot_df[GROWTH_COL].apply(lambda val: BAD_COLOUR if val > 0 else GOOD_COLOUR)

    return plot_df


def _add_hover_tool(plot):
    hover_tool = HoverTool(tooltips=TOOL_TIPS)
    plot.add_tools(hover_tool)


def stlye_axes(plot, major_label_fontsize="10pt"):
    plot.xaxis.formatter = NumeralTickFormatter(format="0.[0] a")
    plot.xgrid.grid_line_color = "White"
    plot.ygrid.grid_line_alpha = 0
    plot.axis.axis_label_text_font_size = "12pt"
    plot.axis.major_label_text_font_size = major_label_fontsize


def add_legend(plot, legend_items):
    # Adding legend below plot
    plot.legend.visible = False
    legend = Legend(items=legend_items, location="center", orientation="horizontal", margin=2, padding=2)
    plot.add_layout(legend, 'below')


def generate_volume_perf_plot(plot_df):
    plot = figure(
        plot_height=None, plot_width=None,
        sizing_mode="scale_both",
        x_axis_label=f"Requests Opened since {REFERENCE_DATE}",
        x_range=(0, plot_df.head(PLOT_CODES)[OPEN_COUNT_COL].max() * 1.05),
        y_range=plot_df.head(PLOT_CODES)[CODE_COL].values[::-1],
        background_fill_color=BACKGROUND_COLOUR,
        toolbar_location=None,
    )

    plot.hbar_stack(
        [OPEN_WITHIN_TARGET_COL, OUTSIDE_TARGET_COL], y=CODE_COL,
        height=0.8, color=[GOOD_COLOUR, BAD_COLOUR],
        source=plot_df.head(PLOT_CODES),
        legend_label=["Within Target", "Outside Target"]
    )

    _add_hover_tool(plot)
    stlye_axes(plot)
    add_legend(plot, plot.legend.items)

    plot_html = file_html(plot, CDN, f"Business Continuity Service Delivery Request Type Within Target")

    return plot_html


def generate_volume_growth_plot(plot_df):
    plot = figure(
        plot_height=None, plot_width=None,
        sizing_mode="scale_both",
        x_axis_label=f"Requests Backlog Growth since {REFERENCE_DATE}",
        y_range=plot_df.head(PLOT_CODES)[CODE_COL].values[::-1],
        background_fill_color=BACKGROUND_COLOUR,
        toolbar_location=None,
    )

    plot.hbar(
        right=GROWTH_COL, y=CODE_COL,
        height=0.8, color=GROWTH_COLOUR_COL,
        source=plot_df.head(10)
    )

    _add_hover_tool(plot)
    stlye_axes(plot)

    # dummy plots to create legend
    growth_bar = plot.hbar(right=[0], y=[plot_df[GROWTH_COL].iloc[0]], color=BAD_COLOUR)
    shrink_bar = plot.hbar(right=[0], y=[plot_df[GROWTH_COL].iloc[0]], color=GOOD_COLOUR)

    # adding legend beloew plot
    legend_items = [("Nett Growth", [growth_bar]),
                    ("Nett Shrink", [shrink_bar])]
    add_legend(plot, legend_items)

    plot_html = file_html(plot, CDN, f"Business Continuity Service Delivery Request Type Nett Growth")

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
    request_data_df = service_delivery_latest_values_to_minio.get_data(
        SD_DEPT_METRICS,
        secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"]
    )
    logging.info("...Fetch[ed] SD data.")

    depts = DEPARTMENTS + [CITY_VALUE]
    for direct, dept in depts:
        logging.info(f"Generat[ing] plot for '{direct}'-'{dept}'")

        logging.debug(f"service_request_df.shape={request_data_df.shape}")
        logging.info(f"Filter[ing] data...")
        filtered_df = (request_data_df.query(
            f"(department == @dept)"
        ) if dept else request_data_df).copy()
        logging.info(f"...Filter[ed] data")
        logging.debug(f"filtered_df.shape={filtered_df.shape}")

        logging.info("Mung[ing] data for plotting...")
        metric_plot_df = generate_plot_df(filtered_df)
        logging.info("...Mung[ed] data")

        logging.info(f"Generat[ing] Volume Performance Plot...")
        volume_perf_plot_html = generate_volume_perf_plot(metric_plot_df)
        logging.info(f"...Generat[ed] Volume Performance Plot")

        logging.info(f"Generat[ing] Volume Growth Plot...")
        volume_growth_plot_html = generate_volume_growth_plot(metric_plot_df)
        logging.info(f"...Generat[ed] Volume Growth Plot")

        logging.info("Writ[ing] to Minio...")
        directorate_file_prefix = direct.lower().replace(" ", "_") if direct else None
        department_file_prefix = dept.lower().replace(" ", "_") if dept else None

        plot_filename_prefix = "city"
        plot_filename_prefix = (f"{plot_filename_prefix}_{directorate_file_prefix}"
                                if directorate_file_prefix else plot_filename_prefix)
        plot_filename_prefix = (f"{plot_filename_prefix}_{department_file_prefix}"
                                if department_file_prefix else plot_filename_prefix)

        volume_perf_plot_filename = f"{SD_PREFIX}_{plot_filename_prefix}_{VOLUME_PERF_PLOT_SUFFIX}"
        service_request_timeseries_plot_widget_to_minio.write_to_minio(volume_perf_plot_html,
                                                                       volume_perf_plot_filename,
                                                                       secrets["minio"]["edge"]["access"],
                                                                       secrets["minio"]["edge"]["secret"])

        volume_growth_plot_filename = f"{SD_PREFIX}_{plot_filename_prefix}_{VOLUME_GROWTH_PLOT_SUFFIX}"
        service_request_timeseries_plot_widget_to_minio.write_to_minio(volume_growth_plot_html,
                                                                       volume_growth_plot_filename,
                                                                       secrets["minio"]["edge"]["access"],
                                                                       secrets["minio"]["edge"]["secret"])
        logging.info("...Wr[ote] to Minio")
