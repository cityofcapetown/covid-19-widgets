"""
This script plots the vaccine rollout time series
"""

__author__ = "Colin Anthony"

# base imports
import json
import logging
import os
import pathlib
import sys
# external imports
from db_utils import minio_utils
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from spv_plot_doubling_time import write_html_to_minio
from vaccine_rollout_plots import minio_to_df, daily_resample, VACC_PLOT_PREFIX

# minio settings
READER = "csv"
COVID_BUCKET = "covid"
TIME_SERIES_PREFIX = "data/private/staff_vaccine/time_series/"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE

# infile
STAFF_TIME_SERIES = "staff-vaccination-time-series.csv"

# outfiles
OUTFILE_PREFIX = "staff-vaccination-timeseries-plot"

AGG_LEVEL = "aggregation_level"
AGG_LEVEL_NAME = "aggregation_level_names"

VAX_DATE = "Vaccination date"
TOP_LEVEL = "city_wide"
DIRECTORATE = "Directorate"
DEPARTMENT = "Department"
SUBDISTRICT = "subdistrict"
BRANCH = "Branch"

# group the variables for the quad plots
PLOT_LEVELS = [TOP_LEVEL, DIRECTORATE, DEPARTMENT, BRANCH]

TOTAL_SUFF = "total"
WILLING_SUFF = "willing"
VACCINATED_CUMSUM = "vaccinated_cumsum"
VACCINATED_REL = "vaccinated_relative"
VACCINATED_PERCENT = "vaccinated_percent"

TOTAL_COUNT = f"{VACCINATED_CUMSUM}_{TOTAL_SUFF}"
TOTAL_PERC = f"{VACCINATED_PERCENT}_{TOTAL_SUFF}"

STAFF_VACC = "Staff Vaccinated"
TOTAL_COUNT_LAB = f"{STAFF_VACC} cumulative count"
TOTAL_PERC_LAB = f"{STAFF_VACC} percent"

TITLE_PREFIX = "Vaccination rollout:"


def plot_vaccine_rollout(df, ylabel):
    """
    function to make doubling time plot
    """
    df[VACCINATED_PERCENT] = round(df[VACCINATED_REL] * 100, 4)
    
    plot_lines = df[AGG_LEVEL_NAME].unique()
    
    # Create figure with secondary y-axis
    fig = make_subplots()

    for line_trace in plot_lines:
        agg_name_df = df.query(f"{AGG_LEVEL_NAME} == @line_trace").copy() 
        x_date = agg_name_df[VAX_DATE]
        if ylabel == TOTAL_COUNT_LAB:
            y_vals = agg_name_df[VACCINATED_CUMSUM]
        else:
            y_vals = agg_name_df[VACCINATED_PERCENT]
        
        # Add the traces
        fig.add_trace(
            go.Scatter(x=x_date, y=y_vals, name=line_trace, line=dict(width=2)),
            secondary_y=False,
        )
    
    # make the legend within the plot
    fig.update_layout(
        plot_bgcolor="white",
        legend=dict(
            x=0.01,
            y=0.8,
            traceorder='normal',
            font=dict(
                size=12),
        ),
        hovermode="x",
        margin={dim: 10 for dim in ("l", "r", "b", "t")},
    )

    # set the grid to white
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(showgrid=False, gridwidth=1, gridcolor='lightgrey')

    # Set x-axis title
    fig.update_xaxes(title_text="")

    # Set y-axes titles
    fig.update_yaxes(title_text=ylabel)

    return fig



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # secrets var
    SECRETS_PATH_VAR = "SECRETS_PATH"

    # Loading secrets
    logging.info(f"Fetch[ing] secrets")
    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        logging.info("Trying to load from file")
        secrets_file = pathlib.Path("/home/jovyan/secrets/secrets.json").resolve()
        if not secrets_file.exists():
            print("could not find your secrets")
            sys.exit(-1)
        else:
            secrets = json.load(open(secrets_file))
            logging.info("Loaded secrets from file")
    else:
        logging.info("Setting secrets variables")
        secrets_file = os.environ[SECRETS_PATH_VAR]
        if not pathlib.Path(secrets_file).exists():
            logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
            sys.exit(-1)
        secrets = json.load(open(secrets_file))
    logging.info(f"Fetch[ed] secrets")

    logging.getLogger('matplotlib.font_manager').disabled = True
    logging.info("Fetch[ing] data")
    staff_ts = minio_to_df(
        minio_filename_override=f"{TIME_SERIES_PREFIX}{STAFF_TIME_SERIES}",
        minio_bucket=COVID_BUCKET,
        data_classification=EDGE_CLASSIFICATION,
        reader=READER
    )
    logging.info("Fetch[ed] data")

    figure_outfiles = []
    for agg_level in PLOT_LEVELS:
        logging.info(f"Plott[ing] {agg_level}")
        staff_ts_filt = staff_ts.query(f"`{AGG_LEVEL}` == @agg_level").copy()

        # resample to daily and ffill missing data
        staff_ts_filt[VAX_DATE] = pd.to_datetime(staff_ts_filt[VAX_DATE])
        staff_ts_filt[VACCINATED_PERCENT] = staff_ts_filt[VACCINATED_REL] * 100
        staff_ts_filt_daily = daily_resample(staff_ts_filt, AGG_LEVEL_NAME)

        # plot the data
        for ylab in [TOTAL_COUNT_LAB, TOTAL_PERC_LAB]:
            plot_html = plot_vaccine_rollout(staff_ts_filt_daily, ylab)
            
            if ylab == TOTAL_COUNT_LAB:
                plot_outfile = f"{OUTFILE_PREFIX}_{agg_level}_cumsum.html"
            else:
                plot_outfile = f"{OUTFILE_PREFIX}_{agg_level}_percent.html"
            
            figure_outfiles.append((plot_html, plot_outfile))

    for figure, region_outfile in figure_outfiles:
        write_html_to_minio(
            plotly_fig=figure,
            outfile=region_outfile,
            prefix=VACC_PLOT_PREFIX,
            bucket=COVID_BUCKET,
            secret_access=secrets["minio"]["edge"]["access"],
            secret_secret=secrets["minio"]["edge"]["secret"],
        )

    logging.info(f"Done")
