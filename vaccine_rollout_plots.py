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
import tempfile
# external imports
from db_utils import minio_utils
from matplotlib import dates as mdates
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sns.set()

# minio settings
READER = "csv"
COVID_BUCKET = "covid"
TIME_SERIES_PREFIX = "data/private/staff_vaccine/time_series/"
VACC_PLOT_PREFIX = "widgets/private/staff_vaccine/"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE

# infile
STAFF_TIME_SERIES = "staff-vaccination-time-series.csv"
STAFF_WILLING_TIME_SERIES = "staff-vaccination-time-series-willing.csv"

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
STAFF_TYPE = "staff_type"
RISK = "risk_score"

# group the variables for the quad plots
PLOT_LEVELS = [TOP_LEVEL, DIRECTORATE, DEPARTMENT, SUBDISTRICT, BRANCH, STAFF_TYPE]

TOTAL_SUFF = "total"
WILLING_SUFF = "willing"
TOTAL_STAFF = "total_staff"
FACILITY = "facility"
POSITION = "Position"
VACCINATED_CUMSUM = "vaccinated_cumsum"
VACCINATED_REL = "vaccinated_relative"
VACCINATED_PERCENT = "vaccinated_percent"
FIX_COLS = [AGG_LEVEL, AGG_LEVEL_NAME, TOTAL_STAFF, DIRECTORATE, DEPARTMENT, BRANCH, FACILITY, POSITION]

TOTAL_COUNT = f"{VACCINATED_CUMSUM}_{TOTAL_SUFF}"
WILLNIG_COUNT = f"{VACCINATED_CUMSUM}_{WILLING_SUFF}"
TOTAL_PERC = f"{VACCINATED_PERCENT}_{TOTAL_SUFF}"
WILLNIG_PERC = f"{VACCINATED_PERCENT}_{WILLING_SUFF}"

STAFF_VACC = "Staff Vaccinated"
TOTAL_COUNT_LAB = f"{STAFF_VACC} count"
TOTAL_PERC_LAB = f"{STAFF_VACC} percent"
WILLNIG_COUNT_LAB = f"{STAFF_VACC} (Willing) count"
WILLNIG_PERC_LAB = f"{STAFF_VACC} (Willing) percent"
TITLE_PREFIX = "Vaccination rollout:"


def minio_to_df(minio_filename_override, minio_bucket, data_classification, reader="csv"):
    logging.debug("Pulling data from Minio bucket...")
    if reader == "csv":
        file_reader = pd.read_csv
    elif reader == "parquet":
        file_reader = pd.read_parquet
    else:
        logging.error("reader is not 'csv' or 'parquet")
        sys.exit(-1)
    with tempfile.NamedTemporaryFile() as temp_data_file:
        result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                           minio_filename_override=minio_filename_override,
                                           minio_bucket=minio_bucket,
                                           data_classification=data_classification,
                                           )
        if not result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
            df = file_reader(temp_data_file.name)

            return df


def plot_timeseries(plot_df, plot_df_willing, label_name, out_name):
    label_name_cln = label_name.replace("_", " ").capitalize()
    formatter = mdates.DateFormatter('%y/%m/%d')
    locator = mdates.WeekdayLocator(interval=1)

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(ncols=2, nrows=2, sharex=True, figsize=(20, 8))
    sns.lineplot(data=plot_df, x=VAX_DATE, y=VACCINATED_CUMSUM, hue=AGG_LEVEL_NAME, ax=ax1)
    sns.lineplot(data=plot_df, x=VAX_DATE, y=VACCINATED_PERCENT, hue=AGG_LEVEL_NAME, ax=ax2)
    sns.lineplot(data=plot_df_willing, x=VAX_DATE, y=VACCINATED_CUMSUM, hue=AGG_LEVEL_NAME, ax=ax3)
    sns.lineplot(data=plot_df_willing, x=VAX_DATE, y=VACCINATED_PERCENT, hue=AGG_LEVEL_NAME, ax=ax4)

    legend_list = []
    for ax in [ax1, ax2, ax3, ax4]:
        ax.set_xlabel("")
        ax.xaxis.set_major_formatter(formatter)
        ax.xaxis.set_major_locator(locator)
        ax.tick_params(axis='x', rotation=45)
        if label_name != TOP_LEVEL:
            for leg_labels in ax.get_legend_handles_labels():
                legend_list.append(leg_labels)

            new_leg_list = []
            for label in legend_list[1]:
                if "," in label:
                    new_lab = (label.strip("\(\)").split(",")[1]).strip()
                else:
                    new_lab = label
                new_leg_list.append(new_lab)
            ax.legend(handles=legend_list[0], labels=new_leg_list, loc=2, title=label_name_cln)
        else:
            ax.legend(loc=2)

    ax1.set_ylabel(TOTAL_COUNT_LAB)
    ax2.set_ylabel(TOTAL_PERC_LAB)
    ax3.set_ylabel(WILLNIG_COUNT_LAB)
    ax4.set_ylabel(WILLNIG_PERC_LAB)

    ax2.set_ylim(0, 100)
    ax4.set_ylim(0, 100)
    plt.suptitle(f"{TITLE_PREFIX} {label_name_cln}")

    with tempfile.TemporaryDirectory() as tempdir:
        outfile_name = pathlib.Path(tempdir, f"{out_name}.png")
        plt.savefig(str(outfile_name), dpi=96, format='png', facecolor='white', bbox_inches='tight')

        logging.debug(f"writing {outfile_name} to minio")
        result = minio_utils.file_to_minio(
                filename=outfile_name,
                minio_bucket=COVID_BUCKET,
                filename_prefix_override=VACC_PLOT_PREFIX,
                data_classification=EDGE_CLASSIFICATION,
        )

    return result


def plot_risk(plot_df_willing, label_name, out_name):
    label_name_cln = label_name.replace("_", " ").capitalize()
    formatter = mdates.DateFormatter('%y/%m/%d')
    locator = mdates.WeekdayLocator(interval=1)

    fig, (ax1, ax2) = plt.subplots(ncols=2, nrows=1, sharex=True, figsize=(20, 8))
    sns.lineplot(data=plot_df_willing, x=VAX_DATE, y=VACCINATED_CUMSUM, hue=AGG_LEVEL_NAME, ax=ax1)
    sns.lineplot(data=plot_df_willing, x=VAX_DATE, y=VACCINATED_PERCENT, hue=AGG_LEVEL_NAME, ax=ax2)

    legend_list = []
    for ax in [ax1, ax2]:
        ax.set_xlabel("")
        ax.xaxis.set_major_formatter(formatter)
        ax.xaxis.set_major_locator(locator)
        ax.tick_params(axis='x', rotation=45)
        if label_name != TOP_LEVEL:
            for leg_labels in ax.get_legend_handles_labels():
                legend_list.append(leg_labels)

            new_leg_list = []
            for label in legend_list[1]:
                if "," in label:
                    new_lab = (label.strip("\(\)").split(",")[1]).strip()
                else:
                    new_lab = label
                new_leg_list.append(new_lab)
            ax.legend(handles=legend_list[0], labels=new_leg_list, loc=2, title=label_name_cln)
        else:
            ax.legend(loc=2)

    ax1.set_ylabel(WILLNIG_COUNT_LAB)
    ax2.set_ylabel(WILLNIG_PERC_LAB)
    ax2.set_ylim(0, 100)

    plt.suptitle(f"{TITLE_PREFIX} {label_name_cln}")

    with tempfile.TemporaryDirectory() as tempdir:
        outfile_name = pathlib.Path(tempdir, f"{out_name}.png")
        plt.savefig(str(outfile_name), dpi=96, format='png', facecolor='white', bbox_inches='tight')

        logging.debug(f"writing {outfile_name} to minio")
        result = minio_utils.file_to_minio(
            filename=outfile_name,
            minio_bucket=COVID_BUCKET,
            filename_prefix_override=VACC_PLOT_PREFIX,
            data_classification=EDGE_CLASSIFICATION,
        )

    return result


def daily_resample(df, target_col):
    df[VAX_DATE] = pd.to_datetime(df[VAX_DATE])
    idx = pd.date_range(df[VAX_DATE].min(), df[VAX_DATE].max(), freq="D")
    groups = df.groupby(target_col)
    collected_df = pd.DataFrame()
    for group_name, group_df in groups:
        new_group_df = group_df.set_index([VAX_DATE]).reindex(idx, fill_value=0, method='ffill').reset_index().copy()
        new_group_df[target_col] = group_name
        for col in FIX_COLS:
            if col in group_df.columns:
                value = list(group_df[col].unique())[0]
                new_group_df[col] = value
        collected_df = pd.concat([collected_df, new_group_df])
    collected_df.rename(columns={"index": "Vaccination date"}, inplace=True)
    collected_df[VACCINATED_CUMSUM].fillna(0, inplace=True)
    collected_df[VACCINATED_PERCENT].fillna(0, inplace=True)
    collected_df.reset_index(drop=True, inplace=True)

    return collected_df


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
    staff_ts, staff_willing_ts = [minio_to_df(
            minio_filename_override=f"{TIME_SERIES_PREFIX}{file}",
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=READER
        ) for file in [STAFF_TIME_SERIES, STAFF_WILLING_TIME_SERIES]]
    logging.info("Fetch[ed] data")

    for df in [staff_ts, staff_willing_ts]:
        df[VACCINATED_PERCENT] = df[VACCINATED_REL] * 100

    for agg_level in PLOT_LEVELS:
        logging.info(f"Plott[ing] {agg_level}")
        staff_ts_filt = staff_ts.query(f"`{AGG_LEVEL}` == @agg_level").copy()
        staff_willing_ts_filt = staff_willing_ts.query(f"`{AGG_LEVEL}` == @agg_level").copy()

        outfile = f"{OUTFILE_PREFIX}_{agg_level}"

        # resample to daily and ffill missing data
        staff_ts_filt[VAX_DATE] = pd.to_datetime(staff_ts_filt[VAX_DATE])
        staff_willing_ts_filt[VAX_DATE] = pd.to_datetime(staff_willing_ts_filt[VAX_DATE])

        staff_ts_filt_daily = daily_resample(staff_ts_filt, AGG_LEVEL_NAME)
        staff_willing_ts_filt_daily = daily_resample(staff_willing_ts_filt, AGG_LEVEL_NAME)

        # plot the data
        plotted = plot_timeseries(
            plot_df=staff_ts_filt_daily,
            plot_df_willing=staff_willing_ts_filt_daily,
            label_name=agg_level,
            out_name=outfile,
        )
        logging.info(f"Plott[ed] {agg_level}")

    # plot risk score timeseries
    staff_willing_ts_risk = staff_willing_ts.query(f"`{AGG_LEVEL}` == @RISK").copy()
    outfile = f"{OUTFILE_PREFIX}_{RISK}"
    plot_risk(staff_willing_ts_risk, RISK, outfile)

    logging.info(f"Done")
