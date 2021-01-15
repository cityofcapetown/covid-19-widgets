"""
This script plots the doubling time from pre-calculated values for the metro and its subdistricts
"""

__author__ = "Colin Anthony"

# base imports
from collections import namedtuple
import json
import logging
import os
import pathlib
import sys
import tempfile
# external imports
from db_utils import minio_utils
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# minio settings
COVID_BUCKET = "covid"
RESTRICTED_PREFIX = "data/private/"
WIDGETS_PREFIX = "widgets/private/"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE

CASES_DT_DISTR = "spv_cases_doubling_time_metro.csv"
DEATHS_DT_DISTR = "spv_deaths_doubling_time_metro.csv"
CASES_DT_SUBDISTR = "spv_cases_doubling_time_metro_subdistricts.csv"
DEATHS_DT_SUBDISTR = "spv_deaths_doubling_time_metro_subdistricts.csv"

CASE_DOUBLE_TIME_PLOT = "spv_cases_percent_change"
DEATH_DOUBLE_TIME_PLOT = "spv_deaths_percent_change"

DATE = "Date"
LEFT_Y_COL = "Relative_Weekly_Delta"
RIGHT_Y_COL = "Daily_Values"
LEFT_Y_LAB = "Percent Change (w/w)"
LEFT_AXIS_COLR = "#74add1"
RIGHT_AXIS_COLR = "#fdae61"

DISTRICT = "District"
SUBDISTRICT = "Subdistrict"
SELECT_CT_METRO = 'CT_Metro_(lag_adjusted)'
DAILY_CASES = "Daily_Cases"
DAILY_DEATHS = "Daily_Deaths"
CUM_CASES = "Cumulative_Cases"
CUM_DEATHS = "Cumulative_Deaths"

# secrets var
SECRETS_PATH_VAR = "SECRETS_PATH"


def minio_csv_to_df(minio_filename_override, minio_bucket, minio_key, minio_secret):
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        minio_result = minio_utils.minio_to_file(
            filename=temp_data_file.name,
            minio_filename_override=minio_filename_override,
            minio_bucket=minio_bucket,
            minio_key=minio_key,
            minio_secret=minio_secret,
            data_classification=EDGE_CLASSIFICATION,
        )
        if not minio_result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
            fetched_df = pd.read_csv(temp_data_file, engine='c', encoding='ISO-8859-1')
            return fetched_df


def write_html_to_minio(plotly_fig, outfile, prefix, bucket, secret_access, secret_secret):
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, outfile)
        logging.debug(f"Push[ing] '{local_path}' to Minio")

        with open(local_path, "w") as plot_file:
            plotly_fig.write_html(
                plot_file,
                include_plotlyjs='directory',
                default_height='100%',
                default_width='100%'
            )
        result = minio_utils.file_to_minio(
            filename=local_path,
            filename_prefix_override=prefix,
            minio_bucket=bucket,
            minio_key=secret_access,
            minio_secret=secret_secret,
            data_classification=EDGE_CLASSIFICATION,
        )
        assert result
        logging.debug(f"Push[ed] '{local_path}' to Minio")

        return result


def preprocess_data(df):
    """
    function to apply datetime conversion and frequency to percentage
    """
    # convert date to date object
    df[DATE] = pd.to_datetime(df[DATE], format='%Y-%m-%d')
    # convert to percent
    df[LEFT_Y_COL] *= 100

    return df


def plot_plotly_plot(x_vals, y1_vals, y2_vals, y2_lab):
    """
    function to make doubling time plot
    """

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add the doubling time trace
    fig.add_trace(
        go.Scatter(x=x_vals, y=y1_vals, name=LEFT_Y_LAB, line=dict(color='#74add1', width=2)),
        secondary_y=False,
    )

    # add the daily cases trace
    fig.add_trace(
        go.Scatter(x=x_vals, y=y2_vals, name=y2_lab, line=dict(color='#fdae61', width=2, dash='dash')),
        secondary_y=True,
    )
    fig.update_traces(hovertemplate='Date: %{x} <br>Value: %{y} <br>')

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
    fig.update_yaxes(title_text=LEFT_Y_LAB,
                     secondary_y=False, color=LEFT_AXIS_COLR)
    fig.update_yaxes(title_text=f"<b>{y2_lab}</b>", secondary_y=True, color=RIGHT_AXIS_COLR)

    return fig


def plot_wrapper(df, data_element):
    """
    function to wrap the plotting function
    """
    # get the values to plot
    df[LEFT_Y_COL] = df[LEFT_Y_COL].apply(lambda x: round(x, 2) if not pd.isna(x) else x)

    x_vals = df[DATE].to_list()
    y1_vals = df[LEFT_Y_COL].to_list()
    # convert to rolling mean
    df[data_element.right_y_col] = df[data_element.right_y_col].rolling(7).mean()
    y2_vals = df[data_element.right_y_col].to_list()

    # make the plot
    logging.info(f"Generat[ing] plot")
    right_y_lab = f'{data_element.right_y_label.replace("_", " ")} (7 day avr)<br>adjusted for reporting lag</br>'
    doubling_time_plot = plot_plotly_plot(x_vals, y1_vals, y2_vals, right_y_lab)
    logging.info(f"Generat[ed] plot")

    return doubling_time_plot


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

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

    logging.info(f"Generat[ing] data objects")
    data = namedtuple("data", "dist_infile subdist_infile right_y_col right_y_label outfile")
    cases_data = data(
        dist_infile=CASES_DT_DISTR, subdist_infile=CASES_DT_SUBDISTR,
        right_y_col=RIGHT_Y_COL, right_y_label=DAILY_CASES,
        outfile=CASE_DOUBLE_TIME_PLOT
    )
    deaths_data = data(
        dist_infile=DEATHS_DT_DISTR, subdist_infile=DEATHS_DT_SUBDISTR,
        right_y_col=RIGHT_Y_COL, right_y_label=DAILY_DEATHS,
        outfile=DEATH_DOUBLE_TIME_PLOT
    )
    logging.info(f"Generat[ed] data objects")

    for data_element in [cases_data, deaths_data]:
        dist_infile = data_element.dist_infile
        subdist_infile = data_element.subdist_infile

        # read in the data
        logging.info(f"Fetch[ing] data from mino")
        spv_district_rates_df = minio_csv_to_df(
            minio_filename_override=f"{RESTRICTED_PREFIX}{dist_infile}",
            minio_bucket=COVID_BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
        spv_subdistrict_rates_df = minio_csv_to_df(
            minio_filename_override=f"{RESTRICTED_PREFIX}{subdist_infile}",
            minio_bucket=COVID_BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
        logging.info(f"Fetch[ed] data from mino")

        # preprocess data
        logging.info(f"Preprocess[ing] the district and subdistrict dataframes")
        spv_district_rates_df = preprocess_data(spv_district_rates_df)
        spv_subdistrict_rates_df = preprocess_data(spv_subdistrict_rates_df)
        logging.info(f"Preprocess[ing] the district and subdistrict dataframes")

        logging.info(f"Collect[ing] list of dataframes for all regions to plot")
        plot_dataframes = []
        # select the CT metro
        df_metro = spv_district_rates_df.query(f"{DISTRICT} == @SELECT_CT_METRO").copy()
        plot_dataframes.append((SELECT_CT_METRO, df_metro))
        # add the subdistrict dataframes
        subdistricts = sorted(spv_subdistrict_rates_df[SUBDISTRICT].unique())
        for subdistrict_name in subdistricts:

            plot_dataframes.append(
                (subdistrict_name, spv_subdistrict_rates_df.query(f"{SUBDISTRICT} == @subdistrict_name").copy())
            )
        logging.info(f"Collect[ed] list of dataframes for all regions to plot")

        for region, region_df in plot_dataframes:
            # get the plot html
            logging.info(f"Generat[ing] plot for {region}")
            doubling_time_plot = plot_wrapper(region_df, data_element)
            logging.info(f"Generat[ed] plot for {region}")

            # write to minio
            region = region.replace("_(lag_adjusted)", "")
            region_outfile = f"{data_element.outfile}_{region}.html"
            logging.debug(f"Push[ing] '{region_outfile}' to Minio")

            write_html_to_minio(
                plotly_fig=doubling_time_plot,
                outfile=region_outfile,
                prefix=WIDGETS_PREFIX,
                bucket=COVID_BUCKET,
                secret_access=secrets["minio"]["edge"]["access"],
                secret_secret=secrets["minio"]["edge"]["secret"],
            )
            logging.debug(f"Push[ed] '{region_outfile}' to Minio")

    logging.info(f"Done")
