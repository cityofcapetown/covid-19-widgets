import json
import logging
import os
import pprint
import sys
import tempfile

from bokeh.embed import file_html
from bokeh.models import HoverTool, DatetimeTickFormatter
from bokeh.plotting import figure
from bokeh.resources import CDN
from db_utils import minio_utils
import pandas

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_RESTRICTED_PREFIX = "data/private/"
HR_DATA_FILENAME = "business_continuity_org_unit_statuses.csv"

DATE_COL_NAME = "Date"
STATUS_COL = "Categories"
SUCCINCT_STATUS_COL = "SuccinctStatus"
ABSENTEEISM_RATE_COL = "Absent"
COVID_SICK_COL = "CovidSick"

TZ_STRING = "Africa/Johannesburg"
ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M"

WORKING_STATUS = "working"
NOT_WORKING_STATUS = "not-working"
STATUSES_TO_SUCCINCT_MAP = {
    "Working remotely (NO Covid-19 exposure)": WORKING_STATUS,
    "At work (on site)": WORKING_STATUS,
    "On leave": NOT_WORKING_STATUS,
    "On suspension": NOT_WORKING_STATUS,
    "Absent from work (unauthorised)": NOT_WORKING_STATUS,
    "Quarantine leave – working remotely": WORKING_STATUS,
    "Quarantine leave – unable to work remotely": NOT_WORKING_STATUS,
    "Quarantine leave – working remotely, Covid-19 exposure / isolation": WORKING_STATUS,
    "Sick (linked to Covid-19)": NOT_WORKING_STATUS,
    "Sick (NOT linked to Covid-19)": NOT_WORKING_STATUS,
    "On Lockdown leave – unable to work remotely": NOT_WORKING_STATUS,
    "On Lockdown leave – able to work remotely": NOT_WORKING_STATUS
}
COVID_SICK = "Sick (linked to Covid-19)"

WIDGETS_RESTRICTED_PREFIX = "widgets/staging/business_continuity_"
PLOT_FILENAME = "hr_absenteeism_plot.html"


def get_data(minio_key, minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_datafile:
        minio_utils.minio_to_file(
            filename=temp_datafile.name,
            minio_filename_override=DATA_RESTRICTED_PREFIX + minio_key,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )

        data_df = pandas.read_csv(temp_datafile.name)

    data_df[DATE_COL_NAME] = pandas.to_datetime(data_df[DATE_COL_NAME])
    logging.debug(f"data_df.columns=\n{data_df.columns}")
    logging.debug(
        f"data_df.columns=\n{pprint.pformat(data_df.dtypes.to_dict())}"
    )

    return data_df


def make_statuses_succinct_again(hr_df):
    hr_df[SUCCINCT_STATUS_COL] = hr_df[STATUS_COL].apply(STATUSES_TO_SUCCINCT_MAP.get)
    logging.debug(f"hr_df.head(5)=\n{hr_df.head(5)}")

    succinct_df = hr_df.groupby(DATE_COL_NAME, SUCCINCT_STATUS_COL).sum().reset_index()
    logging.debug(f"succinct_df=\n{succinct_df}")

    return succinct_df


def get_plot_df(succinct_hr_df):

    def get_col_rate(series, key):
        counts = series.value_counts(normalize=True)

        return counts[key]

    plot_df = (
        succinct_hr_df.groupby(DATE_COL_NAME)
                      .apply(
                        lambda data_df: pandas.DataFrame({
                            ABSENTEEISM_RATE_COL: data_df[SUCCINCT_STATUS_COL].apply(get_col_rate, NOT_WORKING_STATUS),
                            COVID_SICK_COL: data_df[STATUS_COL].apply(get_col_rate, COVID_SICK),
                        }))
    ).reset_index().drop("level_2", axis='columns')

    return plot_df


def generate_plot(plot_df, start_date="2020-03-01", sast_tz='Africa/Johannesburg'):
    start_date = pandas.Timestamp(start_date, tz=sast_tz).date()
    end_date = pandas.Timestamp.now(tz=sast_tz).date()

    TOOLTIPS = [
        ("Date", "@Date{%F}"),
        ("Absenteeism Rate", f"@{ABSENTEEISM_RATE_COL}"),
        ("Covid-19 Related illness", f"@{COVID_SICK_COL}")
    ]
    hover_tool = HoverTool(tooltips=TOOLTIPS,
                           formatters={'Date': 'datetime'})
    # Main plot
    line_plot = figure(
        title=None,
        width=None, height=None,
        x_axis_type='datetime', sizing_mode="scale_both",
        x_range=(start_date, end_date), y_axis_label="Amount (ZAR)",
        toolbar_location=None,
    )
    line_plot.add_tools(hover_tool)

    # Line plots
    line_plot.line(x=DATE_COL_NAME, y=ABSENTEEISM_RATE_COL, color='red', source=plot_df)
    line_plot.line(x=DATE_COL_NAME, y=COVID_SICK_COL, color='organge', source=plot_df)

    # X-axis
    line_plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")

    # Legend Location
    line_plot.legend.location = "bottom_left"

    plot_html = file_html(line_plot, CDN, "Business Continuity Absenteeism Time Series")

    return plot_html


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

    logging.info("Fetch[ing] data...")
    hr_transactional_data_df = get_data(HR_DATA_FILENAME,
                                        secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")

    logging.info("Add[ing] succinct status column...")
    hr_succinct_df = make_statuses_succinct_again(hr_transactional_data_df)
    logging.info("...Add[ed] succinct status column.")

    logging.info("Generat[ing] absenteeism plot...")
    absenteeism_df = get_plot_df(hr_succinct_df)
    plot_html = generate_plot(absenteeism_df)
    logging.info("...Generat[ed] absenteeism plot")

    logging.info("Writ[ing] everything to Minio...")
    write_to_minio(plot_html, PLOT_FILENAME,
                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")

    logging.info("...Done!")
