###################
#
# Widget to plot the self assessment OHS cases
#
##################

import datetime
from datetime import date
import json
import math
import logging
import os
import sys
import tempfile

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from db_utils import minio_utils
import holidays
import pandas as pd

import hr_data_last_values_to_minio
from hr_data_last_values_to_minio import WORKING_STATUS, NOT_WORKING_STATUS, directorate_filter_df, merge_df

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

OHS_DATA_FILENAME = "data/private/ohs_data.csv"

DATA_RESTRICTED_PREFIX = "data/private/"

TZ_STRING = "Africa/Johannesburg"
ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M"

WIDGETS_RESTRICTED_PREFIX = "widgets/private/"
PLOT_FILENAME_SUFFIX = "ohs_cases_plot.html"

def get_plot_df(minio_key, minio_access, minio_secret):
    '''
    Read ohs.xlsx file
    '''
    with tempfile.NamedTemporaryFile() as temp_datafile:
        minio_utils.minio_to_file(
            filename=temp_datafile.name,
            minio_filename_override=minio_key,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )
        data_df = pd.read_csv(temp_datafile.name)

    return data_df

def filter_df(ohs_df, directorate_title):
    filtered_df = (
        ohs_df.query(
            f"directorate.str.lower() == '{directorate_title.lower()}'"
        ) if directorate_title != "*" else ohs_df
    )
    logging.debug(f"filtered_df.head(10)=\n{filtered_df.head(10)}")
        
    return filtered_df

def ohs_data_munge(filtered_df):
    '''
    Prepare data for plotting
    '''
    # Groupby date and cases and count the number of cases
    ohs_group_df = filtered_df.groupby(["date_of_diagnosis", "covid-19_test_results"])["created"].count().reset_index()
    # Keep the positive cases (currently no deaths or recovety data)
    ohs_group_pos_df = ohs_group_df.loc[ohs_group_df["covid-19_test_results"] == "Positive"]
    ohs_group_pos_df["date_of_diagnosis"] = pd.to_datetime(ohs_group_pos_df['date_of_diagnosis'], format="%Y-%m-%d")
    ohs_group_pos_df = ohs_group_pos_df.sort_values("date_of_diagnosis")
    # Create column for accumulative sum
    ohs_group_pos_df["acc_sum"] = ohs_group_pos_df["created"].cumsum()
    
    #today = pd.to_datetime("today")
    today = date.today()
    
    # Create an index range starting on 26 March 2020 (day before lockdown)
    idx = pd.date_range('2020-03-26', today)

    ohs_plot_df =  ohs_group_pos_df.set_index('date_of_diagnosis')

    ohs_plot_df = ohs_plot_df[["acc_sum"]]
    
    # Not all dates have values.  Use forward fill to add missing values
    ohs_plot_df = ohs_plot_df.reindex(idx).ffill()

    ohs_plot_df = ohs_plot_df.reset_index()
     
    return ohs_plot_df


def generate_plot(ohs_plot_df, sast_tz='Africa/Johannesburg'):
    fig = go.Figure()

    fig.add_trace(
            go.Bar(
                x=ohs_plot_df["index"], 
                y=ohs_plot_df["acc_sum"], 
                marker_color="#3D85C6",  
                opacity=0.6,
            )
    )

    fig.update_yaxes(
            title="Total Accumulative Positive Cases",
            dtick = 10,
            tickfont=dict(
                size=10,
            )
    )

    fig.update_xaxes(
            title = "date",
            type="date",
            tickfont=dict(
            size=10,
            )
    )

    fig.update_layout(
            plot_bgcolor=('#fff'),
            hoverlabel = dict(
                        bgcolor='black',
                        font=dict(color='white',
                        size=10,
                        family='Arial')
            ),
            xaxis_tickformat = '%Y-%m-%d',
            margin={dim: 10 for dim in ("l", "r", "b", "t")},
    )

    with tempfile.NamedTemporaryFile("r") as temp_html_file:
        fig.write_html(temp_html_file.name,
                       include_plotlyjs='directory',
                       default_height='100%',
                       default_width='100%'
                       )
        html = temp_html_file.read()

    return html


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
    data_df = get_plot_df(OHS_DATA_FILENAME,
                                secrets["minio"]["edge"]["access"],
                                secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")
    
    logging.info("Filter[ing] data...")
    ohs_filter_df = filter_df(data_df, directorate_title)
    logging.info("...Fetch[ed] data.")     

    logging.info("Mung[ing] data for plotting...")
    ohs_plot_df = ohs_data_munge(ohs_filter_df)
    logging.info("...Mung[ed] data")

    logging.info("Generat[ing] Plot...")
    plot_html = generate_plot(ohs_plot_df)
    logging.info("...Generat[ed] Plot")

    logging.info("Writ[ing] everything to Minio...")
    plot_filename = f"{directorate_file_prefix}_{PLOT_FILENAME_SUFFIX}"
    write_to_minio(plot_html, plot_filename,
                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")

    logging.info("...Done!")
