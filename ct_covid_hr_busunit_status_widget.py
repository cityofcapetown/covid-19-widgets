###################
#
# Widget to plot the self assessment of the business units ability to deliver daily tasks
#
##################

import datetime
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

# from hr_bp_emailer import get_data_df, HR_MASTER_FILENAME_PATH, HR_TRANSACTIONAL_FILENAME_PATH


MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

HR_CITYSTATUS_DATA_FILENAME = "data/private/business_continuity_org_unit_statuses.csv"

DATA_RESTRICTED_PREFIX = "data/private/"

DATE_COL_NAME = "Date"
# STATUS_COL = "Categories"
# SUCCINCT_STATUS_COL = "SuccinctStatus"
# COVID_SICK_COL = "CovidSick"
# ABSENTEEISM_RATE_COL = "Absent"
# DAY_COUNT_COL = "DayCount"

TZ_STRING = "Africa/Johannesburg"
ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M"

WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
PLOT_FILENAME_SUFFIX = "hr_unitstatus_plot.html"


def get_plot_df(minio_key, minio_access, minio_secret):
    '''
    Read HR org unit status .csv file
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


def filter_df(hr_df, directorate_title):
    filtered_df = (
        hr_df.query(
            f"Directorate.str.lower() == '{directorate_title.lower()}'"
        ) if directorate_title != "*" else hr_df
    )
    logging.debug(f"filtered_df.head(10)=\n{filtered_df.head(10)}")

    #  Group by Date and the Evaluation and count number of times the Evaluation status appears for the date
    plot_df = filtered_df.groupby(["Date", "Evaluation"]).count()

    # keep the count
    count = plot_df.Directorate.values

    # Calculate the percentagethe Evaluation status appears for the date and rename the "Directorate" column to "percentage"
    plot_df = plot_df.groupby(level=0).apply(lambda x: 100 * x / x.astype(float).sum()).reset_index()
    plot_df = plot_df.rename(columns={"Directorate": "percentage"})
    plot_df.set_index(["Date", "Evaluation"], inplace=True)

    # add column with count
    plot_df["count"] = count

    # add column with org unit lists
    org_unit_lists = filtered_df.groupby(["Date", "Evaluation"]).apply(
        lambda eval_df: ",<br>".join(eval_df['Org Unit Name'].values[:10])
    )
    logging.debug(f"org_unit_lists=\n{org_unit_lists}")
    plot_df["org_unit_names"] = org_unit_lists

    return plot_df.reset_index()


def orgstatus_data_munge(status_df):
    '''
    Prepare data for plotting
    '''
    # Convert to date format and start from 17 Arpil 2020 when HR online form went live
    status_df['Date'] = pd.to_datetime(status_df['Date'], format="%Y-%m-%d")
    # Start on 16 April when the HR online form went live in the City (17th)
    start_date = pd.to_datetime('2020-04-16', format="%Y-%m-%d")
    # Filter for rows > 16 Arpil 2020 and with a StatusCount > 0 (means that there was an assessment done)
    status_df = status_df.loc[(status_df.Date > start_date) & (status_df.StatusCount > 0)]
    # Drop NaN values if any in "Direcotrate" and "Evaluation" if any NaN in rest of dataset convert to blank ("")
    status_df = status_df.dropna(subset=['Directorate', 'Evaluation'])
    status_df = status_df.fillna('')
    # Drop any duplicate values in the dataset, keping the last. Because each staff will have same Evaluation if part of same org unit.
    # Managers also complete form more than once
    status_df = status_df.drop_duplicates(['Date', 'Org Unit Name', 'Directorate', 'Department', 'Branch', 'Section'],
                                          keep='last')
    # Keep three columns
    status_df = status_df[['Date', 'Directorate', 'Evaluation', 'Org Unit Name']].reset_index().drop("index", axis=1)
    logging.debug(f"status_df.head(10)=\n{status_df.head(10)}")
    # Remove whitespace in Evaluation description
    status_df['Evaluation'] = status_df['Evaluation'].str.strip()
    # sort values to prepare sequence for plotting
    status_df = status_df.sort_values(['Date', 'Directorate'], ascending=True)

    plot_df = filter_df(status_df, directorate_title)

    # Create a dataset for each Evaluation status -- this is done due to the way plotly deals with stacked bar charts
    logging.debug(f"{plot_df.head(10)}")
    city_75_plot_df = plot_df.loc[
        plot_df["Evaluation"] == "We can deliver 75% or less of daily tasks",
        # ['Date', 'percentage', 'Evaluation', 'count']
    ]
    city_can_plot_df = plot_df.loc[
        plot_df["Evaluation"] == "We can deliver on daily tasks",
        # ['Date', 'percentage', 'Evaluation', 'count']
    ]
    city_cannot_plot_df = plot_df.loc[
        plot_df["Evaluation"] == "We cannot deliver on daily tasks",
        # ['Date', 'percentage', 'Evaluation', 'count']
    ]

    return city_can_plot_df, city_75_plot_df, city_cannot_plot_df


def generate_plot(city_can_plot_df, city_75_plot_df, city_cannot_plot_df, sast_tz='Africa/Johannesburg'):
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=city_can_plot_df.Date,
        y=city_can_plot_df.percentage,
        name="We can deliver on daily tasks",
        marker_color="#3D85C6",
        opacity=0.6,
        customdata=city_can_plot_df[["count", "org_unit_names"]].values,
        hovertemplate=
        '<b>Date</b>: %{x} <br>' +
        '<b>Status</b>: We can deliver on daily tasks <br>' +
        '<b>Percentage</b>: %{y:.2f}% <br>' +
        '<b>Nr. of Org Units</b>: %{customdata[0]} <br>' +
        '<b>Top Org Units</b>: %{customdata[1]}' +
        '<extra></extra>'
    ),
    )

    fig.add_trace(go.Bar(
        x=city_75_plot_df.Date,
        y=city_75_plot_df.percentage,
        name="We can deliver 75% or less of daily tasks",
        marker_color="#D0D0D0",
        opacity=0.6,
        customdata=city_75_plot_df[["count", "org_unit_names"]].values,
        hovertemplate=
        '<b>Date</b>: %{x}<br>' +
        '<b>Status</b>: We can deliver 75% or less of daily tasks<br>' +
        '<b>Percentage</b>: %{y:.2f}% <br>' +
        '<b>Nr. of Org Units</b>: %{customdata[0]} <br>' +
        '<b>Top Org Units</b>: %{customdata[1]}' +
        '<extra></extra>'
    ),
    )

    fig.add_trace(go.Bar(
        x=city_cannot_plot_df.Date,
        y=city_cannot_plot_df.percentage,
        name="We cannot deliver on daily tasks",
        marker_color="firebrick",
        opacity=0.6,
        customdata=city_cannot_plot_df[["count", "org_unit_names"]].values,
        hovertemplate=
        '<b>Date</b>: %{x}<br>' +
        '<b>Status</b>: We cannot deliver on daily tasks<br>' +
        '<b>Percentage</b>: %{y:.2f}% <br>' +
        '<b>Nr. of Org Units</b>: %{customdata[0]} <br>' +
        '<b>Top Org Units</b>: %{customdata[1]}' +
        '<extra></extra>'
    ),
    )

    fig.update_yaxes(
        title="% of Org Units Able to Deliver Daily Tasks",
        dtick=5,
        range=[0, 100],
        tickfont=dict(
            size=10,
        )
    )

    fig.update_xaxes(
        title="date",
        # side="top",
        type="date",
        # dtick = 'd1',
        tickfont=dict(
            size=10
        )
    )

    fig.update_layout(
        barmode='stack',
        plot_bgcolor=('#fff'),
        hoverlabel=dict(
            bgcolor='white',
            font=dict(color='black',
                      size=12,
                      family='Arial')
        ),
        xaxis_tickformat='%Y-%m-%d',

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
    satus_data_df = get_plot_df(HR_CITYSTATUS_DATA_FILENAME,
                                secrets["minio"]["edge"]["access"],
                                secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")

    logging.info("Mung[ing] data for plotting...")
    city_can_plot_df, city_75_plot_df, city_cannot_plot_df = orgstatus_data_munge(satus_data_df)
    logging.info("...Mung[ed] data")

    logging.info("Generat[ing] Plot...")
    plot_html = generate_plot(city_can_plot_df, city_75_plot_df, city_cannot_plot_df)
    logging.info("...Generat[ed] Plot")

    logging.info("Writ[ing] everything to Minio...")
    plot_filename = f"{directorate_file_prefix}_{PLOT_FILENAME_SUFFIX}"
    write_to_minio(plot_html, plot_filename,
                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")

    logging.info("...Done!")
