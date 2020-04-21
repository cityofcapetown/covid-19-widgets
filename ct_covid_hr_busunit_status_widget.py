###################
#
# Dataset used:
#    data/private/business_continuity_org_unit_statuses.csv
#
##################

import json
import math
import logging
import os
import sys
import tempfile
#from db_utils import minio_utils
import pandas as pd
import plotly.express as px
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime

#MINIO_BUCKET = "covid"
#MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_RESTRICTED_PREFIX = "data/private/"
WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
HR_UNITSTATUS_DATA_FILENAME = "business_continuity_org_unit_statuses.csv"
PLOT_FILENAME = "ct_covid_hr_unit_status.html"


def get_data(minio_key): #, minio_access, minio_secret):
    #with tempfile.NamedTemporaryFile() as temp_datafile:
     #   minio_utils.minio_to_file(
     #       filename=temp_datafile.name,
    minio_filename_override=DATA_RESTRICTED_PREFIX + minio_key
     #       minio_bucket=MINIO_BUCKET,
            #minio_key=minio_access,
            #minio_secret=minio_secret,
            #data_classification=MINIO_CLASSIFICATION,
     #   )

    data_df = pd.read_csv(minio_filename_override)

    return data_df


def get_hr_data(status_keyname): #, minio_access, minio_secret):
    orgstatus_data = get_data(HR_UNITSTATUS_DATA_FILENAME) #, minio_access, minio_secret)

    return orgstatus_data


def orgstatus_data_munge(status_df):
    
    # Convert to date format and start from 17 Arpil 2020 when HR online form went live
    status_df['Date'] = pd.to_datetime(status_df['Date'], format = "%Y-%m-%d")
    status_df = status_df.sort_values('Date')    
    start_date = pd.to_datetime('2020-04-16', format = "%Y-%m-%d")
    status_df = status_df.loc[status_df.Date > start_date]

    # Concatenate teh different org structures and group by Date and Directorates
    status_df['bus_unit'] = status_df['Department'] + " | " + status_df['Branch'] + " | " + status_df['Section'] + " - [" + status_df['Org Unit Name'] + "]"
    status_df = status_df.groupby(['Date', 'Directorate','bus_unit', 'Evaluation'])['Department'].count().reset_index()
    status_df = status_df.drop(['Department'], axis=1)
    
    # Convert evaluation statusse to ordinal number
    status_df['unit_status'] = np.where(status_df['Evaluation'] == 'We cannot deliver on daily tasks', 0, 
                                             (np.where(status_df['Evaluation'] == 'We can deliver 75% or less of daily tasks', 1, 2)))

    status_df = status_df.drop_duplicates(['Date', 'Directorate', 'bus_unit'], keep='last')
    status_df = status_df.sort_values(['Date','Directorate','bus_unit'], ascending=True)
    status_df = status_df.loc[status_df['unit_status'].isin([0,1]),:]
    status_df=status_df.reset_index().drop('index', axis=1)

    return status_df


def data_munge(status_df):
    status_df = orgstatus_data_munge(status_df)

    return status_df

def get_hover_text(df_subset):
    
    hovertext = list()
    bus_unit = df_subset['bus_unit'].unique()
    datum = df_subset['Date'].unique()

    for b_i, b_val in enumerate(bus_unit): 
        hovertext.append(list())

        data = {'Date': datum, 'bus_unit': b_val}
        df_dt = pd.DataFrame(data)

        df_busunit = df_subset.loc[df_subset['bus_unit'] == b_val,:]
        df_busunit = df_busunit.reset_index().drop('index', axis=1)

        df_dt_bu_merge = df_dt.merge(df_busunit, how='left', on = ['Date','bus_unit'])

        for i in range(len(df_dt_bu_merge)):
            b_val=df_dt_bu_merge['bus_unit'][i]
            d_val=df_dt_bu_merge['Date'][i]
            e_val=df_dt_bu_merge['Evaluation'][i]


            hovertext[-1].append(
                        'Org Unit:  {}<br />'
                        '<br />'
                        'Status:     {}<br />'
                        '<br />'.format(b_val, e_val))
            
    return hovertext
    

def generate_plot(status_df):
    
    dir_list = pd.unique(status_df['Directorate'])
    fig = make_subplots(rows=len(dir_list), cols=1)

    colorscale = ['#8B0000', '#ffe6e6']

    for index, dir in enumerate(dir_list):

        df_subset = status_df.loc[status_df['Directorate'] == dir,:]
        df_subset = df_subset.reset_index().drop('index', axis=1)

        hovertext = get_hover_text(df_subset)

        fig.append_trace(   
                    go.Heatmap(
                            z=df_subset.unit_status,
                            x=df_subset.Date,
                            y=df_subset.bus_unit,
                            colorscale=colorscale,
                            xgap=3, 
                            ygap=3, 
                            showscale=False,
                            hoverongaps = False,
                            hoverinfo='text',
                            text=hovertext


                   ), row=index+1, col=1)



        # Update xaxis properties
        fig.update_yaxes(
                    title_text=dir,
                    title_font=dict(
                        size=9
                    ),
                    showline = False, 
                    showgrid = False, 
                    zeroline = False,
                    tickmode='array',
                    ticks = '',
                    tickfont=dict(
                        size=7,
                    ),                     
                    row=index+1, col=1)

        fig.update_xaxes(
                    type="date",
                    dtick = 'd1',
                    showline = False, 
                    showgrid = False, 
                    zeroline = False,
                    ticks = '',
                    side="top",
                    tickfont=dict(
                        size=7,
                    ),        
                    row=index+1, col=1)


    fig.update_layout(
        title = 'Business Units Assessment of Ability to Deliver Daily Task',
        plot_bgcolor=('#fff'),
        hoverlabel = dict(
                bgcolor='black',
                font=dict(color='white',
                         size=10,
                         family='Arial')
                ),
        showlegend = True,
        legend=dict(y=1.05),
        legend_orientation="h",
        legend_font=dict(
                size=7
            ),
         height=1000
    )

    
   # fig.show(config={'displayModeBar': False}) 


    

    return fig


def write_to_minio(html, minio_filename): #, minio_access, minio_secret):
   # with tempfile.TemporaryDirectory() as tempdir:
   #     local_path = os.path.join(tempdir, minio_filename)

        #logging.debug(f"Writing out HTML to '{local_path}'")
   #     with open(local_path, "w") as line_plot_file:
   #         line_plot_file.write(html)

        #logging.debug(f"Uploading '{local_path}' to Minio")
  #      result = minio_utils.file_to_minio(
  #          filename=local_path,
    filename_prefix_override=WIDGETS_RESTRICTED_PREFIX,
    filename = WIDGETS_RESTRICTED_PREFIX + minio_filename
  #          minio_bucket=MINIO_BUCKET,
            #minio_key=minio_access,
            #minio_secret=minio_secret,
            #data_classification=MINIO_CLASSIFICATION,
#        )

    html.write_html(filename,
              include_plotlyjs='directory',
              default_height='100%',
              default_width='100%'
              )
    
    #assert result


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
#    SECRETS_PATH_VAR = "SECRETS_PATH"

#    if SECRETS_PATH_VAR not in os.environ:#
#        sys.exit(-1)

#    secrets_path = os.environ["SECRETS_PATH"]
#    secrets = json.load(open(secrets_path))

    logging.info("Fetch[ing] data...")
    satus_data_df = get_hr_data(HR_UNITSTATUS_DATA_FILENAME)
                                                      # secrets["minio"]["edge"]["access"],
                                                      # secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")

    logging.info("Mung[ing] data for plotting...")
    satus_data_df = data_munge(satus_data_df)
    logging.info("...Mung[ed] data")
        
    logging.info("Generat[ing] Plot...")
    plot_html = generate_plot(satus_data_df)
    logging.info("...Generat[ed] Plot")

    logging.info("Writ[ing] to Minio...")
    write_to_minio(plot_html, PLOT_FILENAME)
                   #secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] to Minio")
