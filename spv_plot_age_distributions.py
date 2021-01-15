"""
This script takes the spv linelist and outputs lag adjusted values for covid cases in tidy format for CT subdistricts
the metro and WC non-metro areas
"""

__author__ = "Colin Anthony"

# base imports
from datetime import timedelta
import json
import logging
import os
import pathlib
import sys
# external imports
from db_utils import minio_utils
import pandas as pd
import plotly.graph_objects as go
# local imports
from spv_plot_doubling_time import minio_csv_to_df, write_html_to_minio


# data settings
COVID_BUCKET = "covid"
RESTRICTED_PREFIX = "data/private/"
WIDGETS_PREFIX = "widgets/private/"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
# infiles
CASES_ADJUSTED_METRO = "spv_cases_age_distribution.csv"
HOSP_ADJUSTED_METRO = "spv_hosp_age_distribution.csv"
ICU_ADJUSTED_METRO = "spv_icu_age_distribution.csv"
DEATHS_ADJUSTED_METRO = "spv_deaths_age_distribution.csv"
# outfiles
CASES_HEATMAP = "spv_cases_by_age_heatmap"
HOSP_HEATMAP = "spv_hosp_by_age_heatmap"
ICU_HEATMAP = "spv_icu_by_age_heatmap"
DEATHS_HEATMAP = "spv_deaths_by_age_heatmap"

CT_CITY = 'City of Cape Town'
DATE = "Date"
COUNT = "count"
EXPORT = "Export.Date"
DIAGNOSIS = "Date.of.Diagnosis"
HOSP = "Admission.Date"
ICU = "Date.of.ICU.Admission"
DEATH = "Date.of.Death"
DISTRICT = "District"
SUBDISTRICT = "Subdistrict"
UNALLOCATED = "Unallocated"
COLOR_MAP = "RdYlBu"
AGE_BAND = "Age_Band"


SECRETS_PATH_VAR = "SECRETS_PATH"

def df_to_heatmap_format(df: pd.DataFrame):
    # pivot the df
    logging.info("Pivot[ing] age bin df")
    plot_heatmap = df.pivot(
        index=[DATE],
        columns=[AGE_BAND],
        values=[COUNT]
    )[COUNT].reset_index().fillna(0)
    logging.info("Pivot[ed] age bin df")

    logging.info("Transform[ing] df for heatmap")
    plot_heatmap_trans = plot_heatmap.transpose().copy()
    # reset df multi index levels
    plot_heatmap_trans = plot_heatmap_trans.reset_index().rename(columns=plot_heatmap_trans.iloc[0]).drop(0, axis=0)
    logging.info("Transform[ed] df for heatmap")

    # convert date values
    logging.info("Convert[ing] date columns")
    all_dates = plot_heatmap_trans.columns.to_list()[1:]

    for col in all_dates:
        plot_heatmap_trans[col] = plot_heatmap_trans[col].astype(int)
    logging.info("Convert[ed] date columns")

    # convert age bin values to sting
    logging.info("Convert[ing] age bin columns to string")
    plot_heatmap_trans[AGE_BAND] = plot_heatmap_trans[AGE_BAND].astype("string")
    logging.info("Convert[ed] age bin columns to string")

    return plot_heatmap_trans


def plotly_heatmap(heatmap_data_values, heatmap_x_axis_labels, heatmap_y_axis_labels, y_label):

    # style layout
    layout = go.Layout(
        title="",
        xaxis=dict(
            title=""
        ),
        yaxis=dict(
            title=y_label,
        )
    )

    # make the heatmap
    fig = go.Figure(
        data=go.Heatmap(
            z=heatmap_data_values,
            x=heatmap_x_axis_labels,
            y=heatmap_y_axis_labels,
            type='heatmap',
            colorscale=COLOR_MAP,
            reversescale=True,
        ),
        layout=layout,
    )

    # update the hovertool with nice lables (<extra></extra> removes the literal "trace: 0" text from the hovertool)
    fig.update_traces(hovertemplate='Date: %{x} <br>Age Band: %{y} <br>Count: %{z}<extra></extra>')

    # set margins
    fig.update_layout(
        plot_bgcolor="white",
        margin={dim: 10 for dim in ("l", "r", "b", "t")},
    )

    return fig


def call_plotly_heatmap(heatmap_df: pd.DataFrame):
    # get the plot values
    heatmap_data_values = heatmap_df.values
    heatmap_x_axis_labels = heatmap_df.columns.to_list()[1:]
    heatmap_y_axis_labels = heatmap_df[AGE_BAND].to_list()

    y_label = AGE_BAND.replace("_", " ")
    heatmap_fig = plotly_heatmap(heatmap_data_values, heatmap_x_axis_labels, heatmap_y_axis_labels, y_label)

    return heatmap_fig


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

    for dict_collection in [
        {"kind": DIAGNOSIS, "infile": CASES_ADJUSTED_METRO, "outfile": CASES_HEATMAP},
        {"kind": HOSP, "infile": HOSP_ADJUSTED_METRO, "outfile": HOSP_HEATMAP},
        {"kind": ICU, "infile": ICU_ADJUSTED_METRO, "outfile": ICU_HEATMAP},
        {"kind": DEATH, "infile": DEATHS_ADJUSTED_METRO, "outfile": DEATHS_HEATMAP}
    ]:
        kind = dict_collection["kind"]
        infile = dict_collection["infile"]
        outfile = dict_collection["outfile"]

        logging.info(f"Fetch[ing] data from minio")
        apv_age_agg_df = minio_csv_to_df(
            minio_filename_override=f"{RESTRICTED_PREFIX}{infile}",
            minio_bucket=COVID_BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
        apv_age_agg_df.drop(columns=["Unnamed: 0"], inplace=True)
        logging.info(f"Fetch[ed] data from minio")

        export_date = pd.to_datetime(apv_age_agg_df[EXPORT].to_list()[0], format='%Y-%m-%d')
        # set max date to "export_date minus 6 days" to drop the last 5 days of data due to reporting lag
        max_date = export_date - timedelta(days=6)
        # convert date format
        apv_age_agg_df.loc[:, DATE] = pd.to_datetime(apv_age_agg_df[kind], format='%Y-%m-%d').dt.date
        df_filt = apv_age_agg_df.query(f"{DATE} < @max_date").copy()

        # add each District df to the iteration list
        heatmap_dfs = []
        district_names = sorted(df_filt[DISTRICT].unique())
        for district_name in district_names:
            if district_name == UNALLOCATED:
                continue
            heatmap_dfs.append(
                (district_name, df_filt.query(f"{DISTRICT} == @district_name and {SUBDISTRICT}.isna()").copy())
            )

        # add each CT Subdistrict df to the iteration list
        ct_subdistricts = df_filt.query(f"{DISTRICT} == @CT_CITY and {SUBDISTRICT}.notna()").copy()
        ct_subdistrict_names = sorted(ct_subdistricts[SUBDISTRICT].unique())
        for subdistrict_name in ct_subdistrict_names:
            if subdistrict_name == UNALLOCATED:
                continue
            heatmap_dfs.append(
                (subdistrict_name, ct_subdistricts.query(f"{SUBDISTRICT} == @subdistrict_name").copy())
            )

        # iterate over all dataframes to make the heatmaps
        for (region, region_df) in heatmap_dfs:
            # append district/subdistrict name to outfile
            region_outfile = f'{outfile}_{region.replace(" ", "_")}.html'

            # get the dataframe into heatmap format
            logging.info(f"transform[ing] dataframe to heatmap format")
            heatmap_df = df_to_heatmap_format(region_df)
            logging.info(f"transform[ed] dataframe to heatmap format")

            # get the heatmap figure
            logging.info(f"Plott[ing] heatmap figure for {region}")
            heatmap_fig = call_plotly_heatmap(heatmap_df)
            logging.info(f"Plott[ed] heatmap figure for {region}")

            # write to minio
            logging.debug(f"Push[ing] '{outfile}' to Minio")
            write_html_to_minio(
                plotly_fig=heatmap_fig,
                outfile=region_outfile,
                prefix=WIDGETS_PREFIX,
                bucket=COVID_BUCKET,
                secret_access=secrets["minio"]["edge"]["access"],
                secret_secret=secrets["minio"]["edge"]["secret"],
            )
            logging.debug(f"Push[ed] '{outfile}' to Minio")

    logging.info(f"Done")
