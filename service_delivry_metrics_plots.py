"""
script to plot backlog and % closed vs total requests opened in period
"""

__author__ = "Colin Anthony"

# base imports
import json
import logging
import os
import pathlib
import sys
import tempfile
# external imports
from bokeh.embed import file_html
from bokeh.models import BoxZoomTool, ColumnDataSource, HoverTool, LabelSet, Range1d
from bokeh.models import ResetTool, Span, PanTool, BoxAnnotation, WheelZoomTool
from bokeh.plotting import figure
from bokeh.resources import CDN
from db_utils import minio_utils
import numpy as np
import pandas as pd

# set bucket constants
COVID_BUCKET = "covid"
RESTRICTED_PREFIX = "data/private/"
WIDGETS_PREFIX_BIPLOT = "widgets/private/business_continuity_service_delivery_metric_biplots/"
WIDGETS_PREFIX_QAUD = "widgets/private/business_continuity_service_delivery_metric_quadrants/"

EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
LAKE_CLASSIFICATION = minio_utils.DataClassification.LAKE

# files
DEPT_SERVICE_METRICS = "business_continuity_service_delivery_department_metrics.csv"

SECRETS_PATH_VAR = "SECRETS_PATH"

# departments
DEPARTMENTS = [
    "Electricity", "Water and Sanitation", "Solid Waste Management",
    "Roads Infrastructure and Management", "Public Housing",
]
CODE = "Code"
BACKLOG_BLUE = "#4c72b0"
STANDARD_RED = "#c44e52"
SEABORN_BGR = "#eaeaf2"
DEFAULT_GREY ="#bababa"

BACKLOG = "backlog"
SERVICE_STD = "service_standard"
TOTAL_OPEN = "total_opened"
BIPLOT_X_LABEL = "Total Requests Opened since 2020-10-12"
SERVICE_STD_LABEL = "Closed within target (%)"
BACKLOG_LAB = "Backlog (# of requests)"
SHOW_MIN = 2
SERVICE_STD_MIN = 80
BACKLOG_MIN = -1100
BACKLOG_MAX = 5100

def minio_csv_to_df(minio_filename_override, minio_bucket, minio_key, minio_secret, data_classification):
    """
        function to pull minio csv file to python dict
        :param minio_filename_override: (str) minio override string (prefix and file name)
        :param minio_bucket: (str) minio bucket name
        :param minio_key: (str) the minio access key
        :param minio_secret: (str) the minio key secret
        :param data_classification: minio classification (edge | lake)
        :return: pandas dataframe
        """
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                           minio_filename_override=minio_filename_override,
                                           minio_bucket=minio_bucket,
                                           minio_key=minio_key,
                                           minio_secret=minio_secret,
                                           data_classification=data_classification,
                                           )
        if not result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
            df = pd.read_csv(temp_data_file)
            return df


def write_to_minio(data, minio_filename, filename_prefix_override, minio_bucket, minio_key, minio_secret):
    """
        write data to minio
        :param data: (obj) the data
        :param minio_filename: (str) the filename
        :param filename_prefix_override: (str) minio override string (prefix and file name)
        :param minio_bucket: (str) minio bucket name
        :param minio_key: (str) the minio access key
        :param minio_secret: (str) the minio key secret
        :return:
        """
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, minio_filename)

        logging.debug(f"Writing out data to '{local_path}'")
        with open(local_path, "w") as plot_file:
            plot_file.write(data)

        logging.debug(f"Uploading '{local_path}' to Minio")
        result = minio_utils.file_to_minio(
            filename=local_path,
            filename_prefix_override=filename_prefix_override,
            minio_bucket=minio_bucket,
            minio_key=minio_key,
            minio_secret=minio_secret,
            data_classification=EDGE_CLASSIFICATION,
        )

        assert result


def make_fig(df, x_col, y_col, x_lab, y_lab, code, best_fit=None):
    """

    function to make interactive two variable bokeh scatter plot
    :param df: plot dataframe
    :param x_col: (str) column name for x axis data
    :param y_col: (str) column name for y axis data
    :param x_lab: (str) name for x axis label
    :param y_lab: (str) name for y axis label
    :param code: (str) column name for request code data
    :param best_fit: best fit line values [xvalues array, yvalues array]
    :return: (obj) bokeh figure
    """
    ds = ColumnDataSource(df)
    
    # set the axis range
    if y_col == SERVICE_STD:
        x_range = None
        y_range = Range1d(0, SERVICE_STD_MIN + 30, bounds="auto")
        scale = "linear"
        hline = Span(location=SERVICE_STD_MIN, dimension='width', line_color=STANDARD_RED, line_dash='dashed', line_width=1, line_alpha=0.9, name="cutoff")

    else:
        x_range = None
        y_range = None
        scale = "linear"
    
    # initialize the figure
    fig = figure(
        plot_height=None,
        plot_width=None,
        sizing_mode="scale_both",
        border_fill_color='white',
        background_fill_color = SEABORN_BGR,
        background_fill_alpha = 1,
        x_axis_label=x_lab, x_axis_type="log", x_axis_location='below',
        y_axis_label=y_lab, y_axis_type=scale, y_axis_location='left',
        x_range=x_range,
        y_range=y_range,
        title=None,
        tools = [BoxZoomTool(), ResetTool(), PanTool(), WheelZoomTool()],
        toolbar_location='right',
    )

    # add the observed count
    points = fig.circle(x=x_col, y=y_col, source=ds, color='color', size=6, name="data")

    if y_col == BACKLOG and best_fit:
        fig.line(x=best_fit[0], y=best_fit[1], color=BACKLOG_BLUE, alpha=0.8, line_dash='dashed', line_width=1.5, name="bestfit")
    
    # set the hovertools
    tooltips = [
        (y_lab, f'@{y_col}{{0.[0] a}}'),
        (x_lab, f'@{x_col}{{0.[0] a}}'),
        (code, f'@{code}'),
    ]
    hover_tool = HoverTool(tooltips=tooltips, mode='mouse', renderers=[points])
    
    # figure aesthetics
    fig.add_tools(hover_tool)
    fig.axis.axis_label_text_font_size = "12pt"
    fig.axis.minor_tick_line_color = None
    fig.grid.grid_line_color = "white"
    if y_col == SERVICE_STD:
        fig.renderers.extend([hline])

    return fig


def quadrant_fig(df, x_col, y_col, x_lab, y_lab, code, label_df, fx=5, fy=5):
    """
    function to make interactive bokeh scatter quadrant plot
    :param df: plot dataframe
    :param x_col: (str) column name for x axis data
    :param y_col: (str) column name for y axis data
    :param x_lab: (str) name for x axis label
    :param y_lab: (str) name for y axis label
    :param code: (str) column name for request code data
    :param label_df: dataframe for annotating labels
    :param fx: (int) annotation x offset
    :param fy: (int) annotation y offset
    :return: (obj) bokeh figure
    """
    ds = ColumnDataSource(df)
    label_ds = ColumnDataSource(label_df)
    vline = Span(location=SERVICE_STD_MIN, dimension='height', line_color=STANDARD_RED, line_dash='dashed', line_width=1, line_alpha=0.9)
    hline = Span(location=1, dimension='width', line_color=BACKLOG_BLUE, line_dash='dashed', line_width=1, line_alpha=0.9)
    
    # set the y_axis range
    x_range = Range1d(0, SERVICE_STD_MIN + 30, bounds="auto")
    y_min = df[y_col].min()
    if y_min > BACKLOG_MIN + 200:
        set_y_min = BACKLOG_MIN
    else:
        set_y_min = y_min - 200

    y_max = df[y_col].max()
    set_y_max = y_max + 200
        
    y_range = Range1d(set_y_max, set_y_min, bounds="auto")
    
    # set the hovertools
    tooltips = [
        (y_lab, f'@{y_col}{{0.[0] a}}'),
        (x_lab, f'@{x_col}{{0.[0] a}}'),
        (code, f'@{code}'),
        ("Requests Opened", f'@{TOTAL_OPEN}'),
        ("Department", f'@department'),
    ]
    hover_tool = HoverTool(tooltips=tooltips, mode='mouse')
    
    # initialize the figure
    fig = figure(
        plot_height=None,
        plot_width=None,
        sizing_mode="scale_both",
        border_fill_color='white',
        background_fill_color = SEABORN_BGR,
        background_fill_alpha = 1,
        x_axis_label=x_lab, x_axis_type="linear", x_axis_location='below',
        y_axis_label=y_lab, y_axis_type="linear", y_axis_location='left',
        x_range=x_range,
        y_range=y_range,
        title=None,
        tools = [BoxZoomTool(), ResetTool(), PanTool(), WheelZoomTool(), hover_tool],
        toolbar_location='below',
    )
    
     # add quadrant colors 
    green_box = BoxAnnotation(bottom=0, top=set_y_min, left=80, right=110, fill_color='green', fill_alpha=0.1)
    orange_box_1 = BoxAnnotation(bottom=0, top=set_y_min, left=0, right=80, fill_color='orange', fill_alpha=0.1)
    orange_box_2 = BoxAnnotation(bottom=set_y_max, top=0, left=80, right=110, fill_color='orange', fill_alpha=0.1)
    red_box = BoxAnnotation(bottom=set_y_max, top=0, left=0, right=80, fill_color='red', fill_alpha=0.1)
    
    # add the observed count
    fig.circle(x=x_col, y=y_col, source=ds, color='color', size="size")
    fig.circle(x=x_col, y=y_col, source=label_ds, color='dept_color', legend_group='department', size="size")
    
    # add the label annotations
    labels = LabelSet(x=SERVICE_STD, y=BACKLOG, text=code,
                      x_offset=fx, y_offset=fy, source=label_ds, render_mode='canvas', level='glyph')
    
    # figure aesthetics
    
    fig.axis.axis_label_text_font_size = "16pt"
    fig.legend.location = "bottom_left"
    fig.axis.minor_tick_line_color = None
    fig.grid.grid_line_color = "white"
    fig.add_layout(labels)
    fig.add_layout(green_box)
    fig.add_layout(orange_box_1)
    fig.add_layout(orange_box_2)
    fig.add_layout(red_box)
    fig.renderers.extend([hline, vline])

    return fig


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')
    # Loading secrets
    logging.info(f"Fetch[ing] secrets")
    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        logging.info("Trying to load from file")
        secrets_file = pathlib.Path("/home/jovyan/secrets/service-turnaround-secrets.json").resolve()
        if not secrets_file.exists():
            print("could not find your secrets")
            sys.exit(-1)
        else:
            secrets = json.load(open(secrets_file))
            logging.info("Loaded secrets from file")
    else:
        logging.info("Setting secrets variables")
        secrets_path = os.environ["SECRETS_PATH"]
        if not pathlib.Path(secrets_path).glob("*.json"):
            logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
            sys.exit(-1)
        secrets = json.load(open(secrets_path))
    logging.info(f"Fetch[ed] secrets")

    logging.info(f"Fetch[ing] {DEPT_SERVICE_METRICS}")
    department_metrics = minio_csv_to_df(
        minio_filename_override=f"{RESTRICTED_PREFIX}{DEPT_SERVICE_METRICS}",
        minio_bucket=COVID_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
        data_classification=EDGE_CLASSIFICATION,
    )
    logging.info(f"Fetch[ed] {DEPT_SERVICE_METRICS}")

    # plot city wide backlog and % closed vs total opened in period
    
    # list of top 10 by volume
    logging.info(f"Plott[ing] City-wide backlog and Service Standard")
    top_10 = department_metrics.sort_values(TOTAL_OPEN, ascending=False).head(10)[CODE].to_list()
    top_n_per_dept = department_metrics.query("department.isin(@DEPARTMENTS)").sort_values(
        TOTAL_OPEN, ascending=False).groupby("department", sort=False).head(SHOW_MIN)[CODE].to_list()
    extra = [request for request in top_n_per_dept if request not in top_10]
    top_10.extend(extra)
    
    # best fit
    x = department_metrics.sort_values(TOTAL_OPEN)[TOTAL_OPEN]
    y = department_metrics.sort_values(TOTAL_OPEN)[BACKLOG]
    best_fit = (np.unique(x), np.poly1d(np.polyfit(x, y, 1))(np.unique(x)))
    
    # create the color codes
    department_metrics["color"] = department_metrics[CODE].apply(lambda val: DEFAULT_GREY if val not in top_10 else BACKLOG_BLUE)
    
    # call the plot function for backlog
    backlog_fig = make_fig(department_metrics, x_col=TOTAL_OPEN, y_col=BACKLOG,
                           x_lab=BIPLOT_X_LABEL, y_lab=BACKLOG_LAB,
                           code=CODE, best_fit=best_fit)
    
    # call the plot function for service standard
    department_metrics["color"] = department_metrics[CODE].apply(lambda val: DEFAULT_GREY if val not in top_10 else STANDARD_RED)
    p_closed_fig = make_fig(department_metrics, x_col=TOTAL_OPEN, y_col=SERVICE_STD,
                            x_lab=BIPLOT_X_LABEL, y_lab=SERVICE_STD_LABEL,
                            code=CODE, best_fit=None)
    logging.info(f"Plott[ed] City-wide backlog and Service Standard")
    
    logging.info(f"push[ing] plot to minio")
    for (outplot, suffix) in ((backlog_fig, BACKLOG), (p_closed_fig, "percent_closed")):
        plot_html = file_html(outplot, CDN, "citywide")
        outfile = f"business_continuity_service_delivery_city_request_{suffix}.html"
        logging.debug(f"writing {outfile} to minio")
        write_to_minio(
            plot_html,
            outfile,
            WIDGETS_PREFIX_BIPLOT,
            COVID_BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
    logging.info(f"push[ed] plot to minio")

    # departments
    logging.info(f"Plott[ing] department backlog and Service Standard")
    for department in DEPARTMENTS:
        dept_df = department_metrics.query("department == @department").copy().fillna(0)
        dpt_top_10 = dept_df.sort_values(TOTAL_OPEN, ascending=False).head(10)[CODE].to_list()
        if dept_df.empty:
            logging.warning(f"{department} is an empty dataframe")
            continue
        directorate = dept_df.directorate.to_list()[0]

        # best fit
        x = dept_df.sort_values(TOTAL_OPEN)[TOTAL_OPEN]
        y = dept_df.sort_values(TOTAL_OPEN)[BACKLOG]
        best_fit = (np.unique(x), np.poly1d(np.polyfit(x, y, 1))(np.unique(x)))

        dept_df["color"] = dept_df[CODE].apply(lambda val: DEFAULT_GREY if val not in dpt_top_10 else BACKLOG_BLUE)
        backlog_fig = make_fig(dept_df, x_col=TOTAL_OPEN, y_col=BACKLOG,
                               x_lab=BIPLOT_X_LABEL, y_lab=BACKLOG_LAB,
                               code=CODE, best_fit=best_fit)

        dept_df["color"] = dept_df[CODE].apply(lambda val: DEFAULT_GREY if val not in dpt_top_10 else STANDARD_RED)
        p_closed_fig = make_fig(dept_df, x_col=TOTAL_OPEN, y_col=SERVICE_STD,
                                x_lab=BIPLOT_X_LABEL, y_lab=SERVICE_STD_LABEL,
                                code=CODE, best_fit=None)

        dept_name = department.strip().replace(" ", "_").lower()
        directorate = directorate.strip().lower().replace(" ", "_")
        logging.info(f"Plott[ing] department backlog and Service Standard")
        
        # write to minio
        logging.info(f"push[ing] department biplots to minio")
        for (outplot, suffix) in ((backlog_fig, BACKLOG), (p_closed_fig, "percent_closed")):
            plot_html = file_html(outplot, CDN, department)
            outfile = f"business_continuity_service_delivery_city_{directorate}_{dept_name}_request_{suffix}.html"
            logging.debug(f"writing {outfile} to minio")
            write_to_minio(
                plot_html,
                outfile,
                WIDGETS_PREFIX_BIPLOT,
                COVID_BUCKET,
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
            )
        logging.info(f"push[ed] department biplots to minio")

    # city wide quadrant fig
    logging.info(f"Creat[ing] Quadrant plots - all departments")
    # add the point size
    department_metrics["size"] = (department_metrics[TOTAL_OPEN] /
                                  department_metrics[TOTAL_OPEN].max() * 20) + 6

    # make df of top 10 for labelling
    label_df = department_metrics.query(f"{CODE}.isin(@top_10)").copy()

    # plot data
    quadrants_plot = quadrant_fig(
        department_metrics, x_col=SERVICE_STD, y_col=BACKLOG, x_lab=SERVICE_STD_LABEL,
        y_lab=BACKLOG_LAB, code=CODE, label_df=label_df, fx=5, fy=5
    )
    logging.info(f"Creat[ed] Quadrant plots - all departments")
    
    # write to minio
    logging.info(f"Push[ing] {outfile} to minio")
    plot_html = file_html(quadrants_plot, CDN, "backlog_service_std")
    suffix = "backlog_service_std"
    outfile = f"business_continuity_service_delivery_city_request_{suffix}.html"
    write_to_minio(
        plot_html,
        outfile,
        WIDGETS_PREFIX_QAUD,
        COVID_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    logging.info(f"Push[ed] {outfile} to minio")
    
    logging.info(f"Creat[ing] Quadrant plots - each department")
    for department in DEPARTMENTS:
        dept_df = department_metrics.query("department == @department").copy()

        # list of top 10 by volume
        dept_top_10 = dept_df.sort_values(TOTAL_OPEN, ascending=False).head(10)[CODE].to_list()
        
        # make df of top 10 for labelling
        dept_label_df = dept_df.query(f"{CODE}.isin(@dept_top_10)").copy()
        dept_df["color"] = dept_df[CODE].apply(lambda val: DEFAULT_GREY if val not in dept_top_10 else BACKLOG_BLUE)

        # plot data
        quadrants_plot = quadrant_fig(dept_df, x_col=SERVICE_STD, y_col=BACKLOG, x_lab=SERVICE_STD_LABEL,
                                      y_lab=BACKLOG_LAB, code=CODE, label_df=dept_label_df)
        
        # write to minio
        logging.info(f"push[ing] {outfile} to minio")
        plot_html = file_html(quadrants_plot, CDN, "backlog_service_std")
        dept_name = department.strip().replace(" ", "_").lower()
        suffix = f"{dept_name}_backlog_service_std"
        outfile = f"business_continuity_service_delivery_request_city_{suffix}.html"
        write_to_minio(
            plot_html,
            outfile,
            WIDGETS_PREFIX_QAUD,
            COVID_BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
        logging.info(f"push[ed] {outfile} to minio")
    
    logging.info(f"Creat[ed] Quadrant plots - each department")
    logging.info(f"Done")
