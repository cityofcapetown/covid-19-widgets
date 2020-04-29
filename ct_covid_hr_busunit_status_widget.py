###################
#
# Dataset used:
#    data/private/business_continuity_org_unit_statuses.csv
#
##################

import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import holidays
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

HR_UNITSTATUS_DATA_FILENAME = "data/private/business_continuity_org_unit_statuses.csv"

WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
PLOT_FILENAME = "hr_unit_status.html"


def get_data(minio_key, minio_access, minio_secret):
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


def orgstatus_data_munge(status_df):
    # Convert to date format and start from 17 Arpil 2020 when HR online form went live
    status_df['Date'] = pd.to_datetime(status_df['Date'], format="%Y-%m-%d")
    status_df = status_df.sort_values('Date')
    start_date = pd.to_datetime('2020-04-16', format="%Y-%m-%d")
    status_df = status_df.loc[(status_df.Date > start_date) & (status_df.StatusCount > 0)]
    status_df = status_df.dropna(subset=['Directorate', 'Evaluation'])
    status_df = status_df.fillna('')

    status_df['bus_unit'] = status_df['Department'] + " | " + status_df['Branch'] + " | " + status_df[
        'Section'] + " - [" + status_df['Org Unit Name'] + "]"   
    status_df = status_df[['Date', 'Directorate', 'bus_unit', 'Evaluation', 'Org Unit Name']]
    status_df['Evaluation'] = status_df['Evaluation'].str.strip()

    # Convert evaluation statusse to ordinal number
    status_df['unit_status'] = np.where(status_df['Evaluation'] == 'We cannot deliver on daily tasks', 0,
                                        (np.where(
                                            status_df['Evaluation'] == 'We can deliver 75% or less of daily tasks', 1,
                                            2)))

    status_df = status_df.drop_duplicates(['Date', 'Directorate', 'bus_unit'], keep='last')
    status_df = status_df.sort_values(['Date','Directorate','bus_unit'], ascending=True)
    status_df = status_df.loc[status_df['unit_status'].isin([0,1]),:]
    status_df = status_df.reset_index().drop('index', axis=1)

    return status_df


def generate_plot(status_df):
    dir_list = pd.unique(status_df['Directorate'])
    fig = make_subplots(rows=len(dir_list), cols=1)

    colorscale = [[0, '#900C3F'],
                  [0.5, '#900C3F'],
                  [0.5, '#A3E4D7'],
                  [1, '#A3E4D7']]

    datum = list(status_df['Date'].unique())
    datum = pd.to_datetime(pd.Series(datum), format="%Y-%m-%d")

    # Removing Sundays and public holidays
    za_holidays = holidays.CountryHoliday("ZA")
    dates_mask = (
            ~datum.isin(za_holidays) &
            (datum.dt.weekday != 6)
    )
    datum = datum[dates_mask]

    datum = datum.dt.strftime("%Y-%m-%d")

    column_names = ['Date', 'Directorate', 'bus_unit', 'Org Unit Name']
    df_dt = pd.DataFrame(columns=column_names)

    for index, directorate in enumerate(dir_list):
        abbrevated_directorate_label = "".join(map(lambda dir_string: dir_string[0], str(directorate).upper().split()))

        df_subset = status_df.loc[status_df['Directorate'] == directorate, :]

        df_subset = df_subset.reset_index().drop('index', axis=1)
        df_subset = df_subset.sort_values(['Date', 'Directorate', 'bus_unit'], ascending=True)

        bus_unit = list(df_subset['bus_unit'].unique())
        y_value = list(df_subset['Org Unit Name'].unique())

        dat = []
        ev = []
        struct = []

        for b_i, b_val in enumerate(bus_unit):

            dat.append(list())
            ev.append(list())
            struct.append(list())
            df_dt = df_dt.iloc[0:0]

            for n, dt in enumerate(datum):
                df_dt_single = pd.DataFrame(
                    {'Date': dt, 'Directorate': directorate, 'bus_unit': b_val, 'Org Unit Name': y_value[b_i]},
                    index=[n])
                df_dt = df_dt.append(df_dt_single, ignore_index=True)

            df_dt = df_dt.reset_index().drop('index', axis=1)

            df_busunit = df_subset.loc[df_subset['bus_unit'] == b_val, :]
            df_busunit = df_busunit.reset_index().drop('index', axis=1)

            df_busunit['Date'] = df_busunit['Date'].astype(str)
            df_dt_bu_merge = df_dt.merge(df_busunit, how='left', on=['Date', 'Directorate', 'bus_unit', 'Org Unit Name'])
            df_dt_bu_merge['unit_status'] = df_dt_bu_merge['unit_status'].fillna('')

            for i in range(len(df_dt_bu_merge)):
                b_val = df_dt_bu_merge['Org Unit Name'][i]
                d_val = df_dt_bu_merge['Date'][i]
                e_val = df_dt_bu_merge['unit_status'][i]

                dat[-1].append(d_val)
                ev[-1].append(e_val)
                struct[-1].append(b_val)

        daat = [item for sublist in dat for item in sublist]
        evalu = [item for sublist in ev for item in sublist]
        yvalu = [item for sublist in struct for item in sublist]
        df_dt_sub = pd.DataFrame({'Date': daat, 'unit_status': evalu, 'Org Unit Name': yvalu})

        hovertext = []

        for b_i, b_val in enumerate(bus_unit):
            hovertext.append(list())
            df_dt = df_dt.iloc[0:0]

            for n, dt in enumerate(datum):
                df_dt_single = pd.DataFrame(
                    {'Date': dt, 'Directorate': directorate, 'bus_unit': b_val, 'Org Unit Name': y_value[b_i]},
                    index=[n])
                df_dt = df_dt.append(df_dt_single, ignore_index=True)

            df_dt = df_dt.reset_index().drop('index', axis=1)

            df_busunit = df_subset.loc[df_subset['bus_unit'] == b_val, :]
            df_busunit = df_busunit.reset_index().drop('index', axis=1)

            df_busunit['Date'] = df_busunit['Date'].astype(str)
            df_dt_bu_merge = df_dt.merge(df_busunit, how='left', on=['Date', 'Directorate', 'bus_unit', 'Org Unit Name'])
            df_dt_bu_merge['unit_status'] = df_dt_bu_merge['unit_status'].fillna('')

            for i in range(len(df_dt_bu_merge)):
                b_val = df_dt_bu_merge['bus_unit'][i]
                d_val = df_dt_bu_merge['Date'][i]
                e_val = df_dt_bu_merge['Evaluation'][i]

                hovertext[-1].append(
                    'Date:  {}<br />'
                    'Org Unit:  {}<br />'
                    '<br />'
                    'Status:     {}<br />'
                    '<br />'.format(d_val, b_val, e_val))

        heatmap = go.Heatmap(
            z=df_dt_sub.unit_status,
            x=df_dt_sub.Date,
            y=df_dt_sub['Org Unit Name'],
            # z=ev,
            # x=datum,
            # y=y_value,
            colorscale=colorscale,
            xgap=3,
            ygap=2,
            showscale=False,
            hoverinfo='text',
            text=hovertext,
        )

        fig.append_trace(heatmap, row=index + 1, col=1)

        # Update xaxis properties
        fig.update_yaxes(
            title_text=abbrevated_directorate_label,
            
            title_font=dict(
                size=12
            ),
            showline=False,
            showgrid=False,
            zeroline=False,
            tickmode='array',
            ticks='',
            showticklabels=False,
            tickfont=dict(
                size=5
            ),
            row=index + 1, col=1)

        fig.update_xaxes(
            tickformat="%Y-%m-%d",
            tickvals=datum,
            nticks=len(datum),
            dtick=1,
            showline=False,
            showgrid=False,
            zeroline=False,
            ticks='',
            side="top",
            tickfont=dict(
                size=7
            ),
            showticklabels=(index == 0),
            row=index + 1, col=1)

    fig.update_layout(
        
        plot_bgcolor=('#fff'),
        hoverlabel=dict(
            bgcolor='black',
            font=dict(color='white',
                      size=10,
                      family='Arial')
        ),

        legend_font=dict(
            size=7
        ),
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

    if SECRETS_PATH_VAR not in os.environ:  #
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("Fetch[ing] data...")
    satus_data_df = get_data(HR_UNITSTATUS_DATA_FILENAME,
                             secrets["minio"]["edge"]["access"],
                             secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")

    logging.info("Mung[ing] data for plotting...")
    satus_data_df = orgstatus_data_munge(satus_data_df)
    logging.info("...Mung[ed] data")

    logging.info("Generat[ing] Plot...")
    plot_html = generate_plot(satus_data_df)
    logging.info("...Generat[ed] Plot")

    logging.info("Writ[ing] to Minio...")
    write_to_minio(plot_html, PLOT_FILENAME,
                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] to Minio")
