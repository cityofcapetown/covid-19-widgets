import json
import logging
import os
import sys
import tempfile

from bokeh.embed import file_html
from bokeh.models import HoverTool, NumeralTickFormatter, DatetimeTickFormatter, Range1d, LinearAxis
from bokeh.plotting import figure
from bokeh.resources import CDN
from db_utils import minio_utils
import holidays
import pandas

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_RESTRICTED_PREFIX = "data/private/"
HR_DATA_FILENAME = "business_continuity_people_status.csv"

DATE_COL_NAME = "Date"
STATUS_COL = "Categories"
SUCCINCT_STATUS_COL = "SuccinctStatus"
ABSENTEEISM_RATE_COL = "Absent"
COVID_SICK_COL = "CovidSick"
DAY_COUNT_COL = "DayCount"

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

WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
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

    return data_df


def make_statuses_succinct_again(hr_df):
    hr_df[SUCCINCT_STATUS_COL] = hr_df[STATUS_COL].apply(STATUSES_TO_SUCCINCT_MAP.get)
    logging.debug(f"hr_df.head(5)=\n{hr_df.head(5)}")

    return hr_df


def get_plot_df(succinct_hr_df):
    succinct_hr_df[DATE_COL_NAME] = succinct_hr_df[DATE_COL_NAME].dt.date
    plot_df = (
        succinct_hr_df.groupby(DATE_COL_NAME)
            .apply(
            lambda data_df: pandas.DataFrame({
                ABSENTEEISM_RATE_COL: [data_df[SUCCINCT_STATUS_COL].value_counts(normalize=True)[NOT_WORKING_STATUS]],
                COVID_SICK_COL: [
                    data_df[STATUS_COL].value_counts(normalize=True)[COVID_SICK] if data_df[STATUS_COL].str.contains(
                        COVID_SICK).any() else 0],
                DAY_COUNT_COL: data_df.shape[0]
            }))
    ).reset_index().drop("level_1", axis='columns')

    # Filtering out holidays
    za_holidays = holidays.CountryHoliday("ZA")
    plot_df = plot_df[
        plot_df[DATE_COL_NAME].apply(lambda date: date not in za_holidays) &
        (pandas.to_datetime(plot_df[DATE_COL_NAME]).dt.weekday != 6)  # Sunday
        ]

    return plot_df


def generate_plot(plot_df, sast_tz='Africa/Johannesburg'):
    start_date = plot_df[DATE_COL_NAME].min()
    end_date = pandas.Timestamp.now(tz=sast_tz).date()

    TOOLTIPS = [
        ("Date", "@Date{%F}"),
        ("Absenteeism Rate", f"@{ABSENTEEISM_RATE_COL}{{0.0 %}}"),
        ("Covid-19 Related illness", f"@{COVID_SICK_COL}{{0.0 %}}"),
        ("Number reported", f"@{DAY_COUNT_COL}{{0 a}}")
    ]
    hover_tool = HoverTool(tooltips=TOOLTIPS,
                           formatters={'Date': 'datetime'})
    # Main plot
    line_plot = figure(
        title=None,
        width=None, height=None,
        x_axis_type='datetime', sizing_mode="scale_both",
        x_range=(start_date, end_date), y_axis_label="Rate (%)",
        y_range=(0, 1.05),
        toolbar_location=None,
    )
    line_plot.add_tools(hover_tool)

    # Adding count on the right
    line_plot.extra_y_ranges = {"count_range": Range1d(start=0, end=plot_df[DAY_COUNT_COL].max() * 1.1)}
    second_y_axis = LinearAxis(y_range_name="count_range", axis_label="Number Assessed")
    line_plot.add_layout(second_y_axis, 'right')

    # Bar plot for counts
    line_plot.vbar(x=DATE_COL_NAME, top=DAY_COUNT_COL, width=5e7, color="blue", source=plot_df,
                   y_range_name="count_range", alpha=0.4)

    # Line plots
    line_plot.line(x=DATE_COL_NAME, y=ABSENTEEISM_RATE_COL, color='red', source=plot_df, line_width=5)
    line_plot.scatter(x=DATE_COL_NAME, y=ABSENTEEISM_RATE_COL, fill_color='red', source=plot_df, size=20, line_alpha=0)

    line_plot.line(x=DATE_COL_NAME, y=COVID_SICK_COL, color='orange', source=plot_df, line_width=5)
    line_plot.scatter(x=DATE_COL_NAME, y=COVID_SICK_COL, fill_color='orange', source=plot_df, size=20, line_alpha=0)

    # axis formatting
    line_plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")

    line_plot.yaxis.formatter = NumeralTickFormatter(format="0 %")
    second_y_axis.formatter = NumeralTickFormatter(format="0 a")

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
