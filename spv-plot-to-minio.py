# base imports
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
import json
import logging
import math
import os
import pathlib
import sys
import tempfile
# external imports
from bokeh.embed import file_html
from bokeh.models import ColumnDataSource, DatetimeTickFormatter, HoverTool, Label, Span 
from bokeh.plotting import figure
from bokeh.resources import CDN
from db_utils import minio_utils
import pandas as pd
from pandas.tseries.offsets import BDay


__author__ = "Colin Anthony"


# set the minio variables
MINIO_BUCKET = 'covid'
WIDGETS_RESTRICTED_PREFIX = "widgets/private/"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE
# set date filters
START = "2020-04-28"
TODAY_DATE = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
MODEL_DATE = pd.to_datetime("2020-06-22")
PLOT_END = TODAY_DATE + timedelta(days=10)
# set the model and dataset
MODEL_FILE_NAME = "wc_model_data.csv"
LATEST_SPV_LAG_ADJUST = "ct-all-cases-lag-adjusted.csv"
# set the output file names
CASE_OUTFILE = 'ct-cov2-daily-cases.html'
ADMIT_OUTFILE = 'ct-cov2-daily-gen-admissions.html'
ICU_OUTFILE = 'ct-cov2-daily-icu-admissions.html'
DEATHS_OUTFILE = 'ct-cov2-daily-deaths.html'

            
def minio_csv_to_df(minio_filename_override, minio_bucket, minio_key, minio_secret):
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                           minio_filename_override=minio_filename_override,
                                           minio_bucket=minio_bucket,
                                           minio_key=minio_key,
                                           minio_secret=minio_secret,
                                           data_classification=MINIO_CLASSIFICATION,
                                           )
        if not result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...") 
            df = pd.read_csv(temp_data_file, engine='c', encoding='ISO-8859-1')
            return df

        
def write_to_minio(data, minio_filename, minio_bucket, minio_key, minio_secret):
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, minio_filename)

        logging.debug(f"Writing out data to '{local_path}'")
        with open(local_path, "w") as line_plot_file:
            line_plot_file.write(data)

        logging.debug(f"Uploading '{local_path}' to Minio")
        result = minio_utils.file_to_minio(
            filename=local_path,
            filename_prefix_override=WIDGETS_RESTRICTED_PREFIX,
            minio_bucket=minio_bucket,
            minio_key=minio_key,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        assert result
        

def make_fig(
    df, x_col, y_col, y_adj_col, 
    y_leg, lab, lab_adj, title, xrange,
    model_x, model_p5, model_p25, model_median, model_p75, model_p95,
    announce_x=False, announce_cases_y=None, wc_label=None,
    ):
    
    if announce_cases_y:
        if df[y_adj_col].max() >= df[announce_cases_y].max():
            y_max = df[y_adj_col].max() * 1.1
        else:
            y_max = df[announce_cases_y].max() * 1.1
    elif not y_adj_col:
        y_max = df[y_col].max() * 1.1
    else:
         y_max = df[y_adj_col].max() * 1.1
            
    ds = ColumnDataSource(df)
    fig = figure(
        background_fill_color='white', border_fill_color='white', 
        x_axis_label='', x_axis_type='datetime', x_axis_location='below',
        y_axis_label=y_leg, y_axis_type='linear', y_axis_location='left',
        width=None, 
        height=None,
        sizing_mode="scale_both",
        x_range=xrange,
        y_range=(0, y_max),
        title=None,
        toolbar_location=None,
    )
    
    # add the observed count
    fig.circle(
        x=x_col, y=y_col, source=ds,
        color='#9e0142', legend_label=lab)
    fig.line(
        x=x_col, y=y_col, source=ds,
        color='#9e0142', line_width=1, legend_label=lab)
    
    # add the adjusted count
    if y_adj_col:
        fig.circle(
            x=x_col, y=y_adj_col, source=ds,
            color='#f46d43', legend_label=lab_adj)
        fig.line(
            x=x_col, y=y_adj_col, source=ds,
            line_dash="dashed", color='#f46d43', line_width=1, legend_label=lab_adj)
    
    # add the median and fill between model bounds
    fig.varea(
        x=model_x, y1=model_p75, y2=model_p95,
        alpha=0.5, fill_color='#74add1', legend_label="WC Model P75-P95")
    fig.circle(
        x=model_x, y=model_median,
        color='#74add1', legend_label="WC Model Median")
    fig.line(
        x=model_x, y=model_median, line_dash="solid",
             color='#74add1', line_width=1, legend_label="WC Model Median")
    fig.varea(
        x=model_x, y1=model_p5, y2=model_p25,
        alpha=0.2, fill_color='#74add1', legend_label="WC Model P5-P25")   
    
    # line for today
    vline = Span(location=TODAY_DATE, dimension='height', line_color='grey', line_width=1, line_alpha=0.5)
    
    my_label = Label(x=TODAY_DATE, y=300, y_units='screen', text=f'Today')
    
    
    # add the WC announcements
    if announce_x:
        fig.circle(
            x=x_col, y=announce_cases_y, source=ds,
            color='#5e4fa2', legend_label=wc_label)
        fig.line(
            x=x_col, y=announce_cases_y, source=ds,
            line_dash="solid",
            color='#5e4fa2', line_width=1, legend_label=wc_label)
        
        tooltips = [
            (lab, f'@{y_col}{{0}}'),
            (lab_adj, f'@{y_adj_col}{{0}}'),
            (wc_label, f"@{announce_cases_y}{{0}}"),
            ("Date", '@Date{%F}')
           ]
        
    elif y_adj_col:
        tooltips = [
            (lab, f'@{y_col}{{0}}'),
            ("Date", '@Date{%F}')
           ]
        
    else:
        tooltips = [
            (lab, f'@{y_col}{{0}}'),
            (lab_adj, f'@{y_adj_col}{{0}}'),
            ("Date", '@Date{%F}')
           ]
    
    fig.xaxis.formatter=DatetimeTickFormatter(days="%d - %b")
    fig.xaxis.major_label_orientation = math.pi / 4
    fig.axis.minor_tick_line_color = None
    fig.legend.location = 'top_left'
    fig.legend.background_fill_alpha = 0.0
    fig.grid.grid_line_color = None
    
    hover_tool = HoverTool(tooltips=tooltips, mode='mouse', formatters={'@Date': 'datetime'})
    fig.add_tools(hover_tool)
    
    fig.add_layout(my_label)
    fig.renderers.extend([vline])
    
    return fig


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        sys.exit(-1)

    logging.info("Setting secrets variables")
    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))
    if not pathlib.Path(secrets_path).glob("*.json"):
        logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
        sys.exit(-1)
    
    # _________________________________________________________________
    # get the model data
    masha_df = minio_csv_to_df(
            minio_filename_override=f"data/private/{MODEL_FILE_NAME}",
            minio_bucket=MINIO_BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
    
    # format date column
    logging.debug(f"Date formatting on model dataframe")
    masha_df.loc[:, "ForecastDate"] = pd.to_datetime(masha_df["ForecastDate"])#.dt.tz_convert("Africa/Johannesburg").dt.tz_localize(None)
    masha_df.loc[:, "TimeInterval"] = pd.to_datetime(masha_df["TimeInterval"])#.dt.tz_convert("Africa/Johannesburg").dt.tz_localize(None)
    masha_df.loc[:, "ForecastDate"]  = masha_df["ForecastDate"].dt.date
    masha_df.loc[:, "TimeInterval"]  = masha_df["TimeInterval"].dt.date
    
    logging.debug(f"Filtering to the model from {MODEL_DATE}")
    masha_filt_df = masha_df.query("ForecastDate == @MODEL_DATE").copy()
    
    # _________________________________________________________________
    # get WC announcement data
    logging.debug(f"Getting the WC announcement data")
    announcements_df = minio_csv_to_df(
        minio_filename_override=f"data/wc_announcement_data.csv",
        minio_bucket="covid-fatalities-management",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    
    # _________________________________________________________________
    # get spv latest
    logging.debug(f"Getting the latest spv data")
    plot_master_df = minio_csv_to_df(
        minio_filename_override=f"data/private/{LATEST_SPV_LAG_ADJUST}",
        minio_bucket=MINIO_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
        
#     # add the WC announcements data
    logging.debug(f"Merging the WC announcement data with the plot dataframe")
    announcements_df.rename(columns={"announcement_date": "Date"}, inplace=True)
    plot_master_df = pd.merge(plot_master_df, announcements_df[["Date", "daily_confirmed", "daily_deaths"]], how='outer', on="Date")
    plot_master_df.sort_values("Date", ascending=True, inplace=True)
    
#     # filter to only the weekdays
#     logging.debug(f"Filtering plot dataframe to only the business days/weekdays")
#     isBusinessDay = BDay().is_on_offset
#     match_series = pd.to_datetime(plot_master_df['Date']).map(isBusinessDay)
#     plot_master_df_wkdays = plot_master_df[match_series].copy()

    # _________________________________________________________________
    # add the model data
    scenario_lookup = {"DEFAULT": "Median",
                       "CurrentCapacity_P5": "P5",
                       "CurrentCapacity_P25": "P25",
                       "CurrentCapacity_P75": "P75",
                       "CurrentCapacity_P95": "P95"
                      }

    model_scenarios = masha_filt_df.groupby("Scenario")
    model_plot_dic = defaultdict(lambda: defaultdict(list))
    for scenario, gdf in model_scenarios:
        if scenario in scenario_lookup.keys(): 
            scenario = scenario_lookup[scenario]
            time = gdf["TimeInterval"].values
            cases = list(gdf["NewInfections"])
            admit = list(gdf["NewGeneralAdmissions"])
            icu = list(gdf["NewICUAdmissions"])
            mort = list(gdf["NewDeaths"])

            model_plot_dic["case"][scenario] = cases
            model_plot_dic["case"]["time"] = time
            model_plot_dic["admit"][scenario] = admit
            model_plot_dic["admit"]["time"] = time
            model_plot_dic["icu"][scenario] = icu
            model_plot_dic["icu"]["time"] = time
            model_plot_dic["mort"][scenario] = mort
            model_plot_dic["mort"]["time"] = time

    # _________________________________________________________________
    # Plot the figure
    logging.debug(f"Plotting the data")
    
    # set the date for bokeh
    plot_master_df["Date"] = pd.to_datetime(plot_master_df["Date"])
    
    # set common labels
    x_range=(pd.to_datetime(START), pd.to_datetime(PLOT_END))
    logging.debug(f"x-axis range set to {x_range}")
    wc_label = "Winde - Daily Cases"
        
    case_fig = make_fig(
        plot_master_df, "Date", "Diagnoses_Count", "Diagnoses_AdjustedCount", 
        "Cases / Day", 'Cases', 'Lag Adjusted Cases', 'Cases', x_range,
        model_plot_dic["case"]["time"], model_plot_dic["case"]["P5"], model_plot_dic["case"]["P25"], model_plot_dic["case"]["Median"], 
        model_plot_dic["case"]["P75"], model_plot_dic["case"]["P95"],
        announce_x=False, # announce_cases_y="daily_confirmed", wc_label=wc_label
    )

    admit_fig = make_fig(
        plot_master_df, "Date", "Admissions_Count", None, 
        "Gen Admissions / Day", 'Gen Admissions', 'Lag Adjusted Admissions', 'Gen Admissions', x_range,
        model_plot_dic["admit"]["time"], model_plot_dic["admit"]["P5"], model_plot_dic["admit"]["P25"], model_plot_dic["admit"]["Median"], 
        model_plot_dic["admit"]["P75"], model_plot_dic["admit"]["P95"],
        announce_x=False,
    )
    
    icu_fig = make_fig(
        plot_master_df, "Date", "ICUAdmissions_Count", None, 
        "ICU Admissions / Day", 'ICU Admissions', 'Lag Adjusted ICU Admissions', 'ICU Admissions', x_range,
        model_plot_dic["icu"]["time"], model_plot_dic["icu"]["P5"], model_plot_dic["icu"]["P25"], model_plot_dic["icu"]["Median"], 
        model_plot_dic["icu"]["P75"], model_plot_dic["icu"]["P95"],
        announce_x=False,
    )
    
    wc_label = "Winde - Daily Deaths"
    mort_fig = make_fig(
        plot_master_df, "Date", "Deaths_Count", "Deaths_AdjustedCount", 
        "Deaths / Day", 'Deaths', 'Lag Adjusted Deaths', 'Deaths', x_range,
        model_plot_dic["mort"]["time"], model_plot_dic["mort"]["P5"], model_plot_dic["mort"]["P25"], model_plot_dic["mort"]["Median"], 
        model_plot_dic["mort"]["P75"], model_plot_dic["mort"]["P95"],
        announce_x=False, # announce_cases_y="daily_deaths", wc_label=wc_label,
    )
    
    # generate html of figure
    logging.debug(f"generating html file from figure")
    cases_html = file_html(case_fig, CDN, "Daily Covid Cases with projections")
    admit_html = file_html(admit_fig, CDN, "Daily Covid General Admissions with projections")
    icu_html = file_html(icu_fig, CDN, "Daily Covid ICU Admissions with projections")
    deaths_html = file_html(mort_fig, CDN, "Daily Covid Deaths with projections")

    plot_tupples = ((cases_html, CASE_OUTFILE),  (admit_html, ADMIT_OUTFILE), (icu_html, ICU_OUTFILE), (deaths_html, DEATHS_OUTFILE))
    
    logging.debug(f"writing html to minio")
    for plot_html, outfile in plot_tupples:
        logging.debug(f"writing {outfile} to minio")
        write_to_minio(
            plot_html, 
            outfile, 
            minio_bucket="covid",
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
        
        logging.debug(f"Done")
    