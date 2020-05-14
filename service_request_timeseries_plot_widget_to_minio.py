import json
import math
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
from bokeh.embed import file_html

from bokeh.plotting import figure
from bokeh.models import Range1d, LinearAxis, Legend, DatetimeTickFormatter, HoverTool
from bokeh.resources import CDN
from bokeh.transform import dodge

import pandas
import pytz
from tqdm.auto import tqdm

tqdm.pandas()

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_BUCKET_NAME = "service-standards-tool.citizen-notifications"
SAST_TZ = pytz.FixedOffset(120)
DATA_START_DATE = pandas.Timestamp("2020-03-01", tz=SAST_TZ)

DATE_COL = "Date"
OPENED_COL = "Opened"
CLOSED_COL = "Closed"
TOTAL_COL = "Total"

DURATION_COL = "Duration"
DURATION_QUANTILE = 0.8
DURATION_WINDOW = 28

PREVIOUS_OPEN_COL = "PreviousOpened"
PREVIOUS_CLOSED_COL = "PreviousClosed"
PREVIOUS_TOTAL_COL = "PreviousTotal"
PREVIOUS_DURATION_COL = "PreviousDurations"
PREVIOUS_TOTAL_WINDOW = 7
PREVIOUS_OFFSET = 365

HOVER_COLS = [DATE_COL,
              OPENED_COL, CLOSED_COL, DURATION_COL,
              PREVIOUS_OPEN_COL, PREVIOUS_CLOSED_COL, PREVIOUS_DURATION_COL]

WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
PLOT_FILENAME_SUFFIX = "service_request_count_plot.html"


def get_service_request_data(minio_access, minio_secret):
    service_request_df = minio_utils.minio_to_dataframe(
        minio_bucket=DATA_BUCKET_NAME,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=minio_utils.DataClassification.CONFIDENTIAL,
        use_cache=True
    )

    return service_request_df


def filter_sr_data(sr_df, start_date, end_date=None, offset_length=0):
    if offset_length != 0:
        offset = pandas.Timedelta(days=offset_length)
        start_date -= offset
        end_date -= offset

    filter_string = "(CreationTimestamp.dt.date >= @start_date)"
    filter_string += " & (CreationTimestamp.dt.date < @end_date)" if end_date is not None else ""
    logging.debug(f"Resulting filter string: '{filter_string}'")
    logging.debug(f"start_date='{start_date}'")
    if end_date is not None:
        logging.debug(f"end_date='{end_date}'")

    return sr_df.query(filter_string)


def get_daily_totals_df(sr_df, data_start_date=DATA_START_DATE):
    daily_count_df = sr_df.resample(
        "1D", on="CreationTimestamp"
    ).count()["NotificationType"].rename(OPENED_COL).to_frame()

    daily_count_df[CLOSED_COL] = sr_df.resample(
        "1D", on="CompletionTimestamp"
    ).count()["NotificationType"]

    daily_count_df[TOTAL_COL] = daily_count_df[OPENED_COL] + daily_count_df[CLOSED_COL]
    daily_count_df[DATE_COL] = daily_count_df.index.to_series().dt.date

    # Getting rid of everything scooped in before start date
    daily_count_df.drop(
        daily_count_df.query(f"{DATE_COL} < @data_start_date").index, inplace=True
    )

    return daily_count_df


def get_p80_window(start_date, sr_df, quantile=DURATION_QUANTILE, window_length=DURATION_WINDOW, offset=0):
    window_start = start_date - pandas.Timedelta(days=window_length)
    filtered_df = filter_sr_data(sr_df, window_start, start_date, offset_length=offset)

    # Calculating durations
    duration_series = filtered_df.CompletionTimestamp.copy()

    # Thresholding completion timestamps to start_date
    threshold_mask = filtered_df.CompletionTimestamp.isna()
    if threshold_mask.sum() < filtered_df.shape[0]:
        threshold_mask |= (filtered_df.CompletionTimestamp.dt.date > start_date)

    duration_series[threshold_mask] = pandas.Timestamp(start_date, tz=pytz.FixedOffset(120))

    duration_series -= filtered_df.CreationTimestamp
    duration_days = (duration_series.dt.total_seconds() / 3600 / 24).round(1)
    p80 = duration_days.quantile(quantile)

    return p80


def get_previous_year_counts(sr_df, previous_window_start, window_length=PREVIOUS_TOTAL_WINDOW):
    previous_df = get_daily_totals_df(sr_df, previous_window_start)

    rolling_df = previous_df.rolling(window_length, on="Date").mean().round(0)
    previous_values_dict = {
        PREVIOUS_OPEN_COL: rolling_df[OPENED_COL].fillna(0).values,
        PREVIOUS_CLOSED_COL: rolling_df[CLOSED_COL].fillna(0).values,
        PREVIOUS_TOTAL_COL: rolling_df[TOTAL_COL].fillna(0).values
    }

    return previous_values_dict


def generate_plot_timeseries(sr_df):
    filtered_df = filter_sr_data(sr_df, DATA_START_DATE)
    logging.debug(f"filtered_df.CreationTimestamp.describe()=\n{filtered_df.CreationTimestamp.describe()}")

    logging.debug("Getting daily totals")
    timeseries_df = get_daily_totals_df(filtered_df)
    logging.debug(f"timeseries_df.describe()=\n{timeseries_df.describe()}")

    logging.debug("Getting P80 values")
    timeseries_df[DURATION_COL] = timeseries_df[DATE_COL].progress_apply(
        get_p80_window, sr_df=sr_df
    )

    logging.debug("Getting previous year's totals")
    previous_window_start = DATA_START_DATE - pandas.Timedelta(days=PREVIOUS_TOTAL_WINDOW)
    previous_window_end = max(sr_df.CompletionTimestamp.max(), pandas.Timestamp.now(tz=SAST_TZ))
    previous_filtered_df = filter_sr_data(sr_df, previous_window_start, previous_window_end,
                                          offset_length=PREVIOUS_OFFSET)
    previous_window_start -= pandas.Timedelta(days=PREVIOUS_OFFSET)

    for col, values in get_previous_year_counts(previous_filtered_df, previous_window_start).items():
        timeseries_df[col] = values[:timeseries_df.shape[0]]

    logging.debug("Getting previous year's duration")
    timeseries_df[PREVIOUS_DURATION_COL] = timeseries_df[DATE_COL].progress_apply(
        get_p80_window, sr_df=sr_df, offset=PREVIOUS_OFFSET
    )
    logging.debug(f"timeseries_df.head(10)=\n{timeseries_df.head(10)}")

    return timeseries_df


def generate_plot(plot_df):
    window_start = plot_df.index.max() - pandas.Timedelta(days=14)
    window_end = plot_df.index.max() + pandas.Timedelta(days=1)

    window_total = plot_df.loc[plot_df.index > window_start,
                               [TOTAL_COL, PREVIOUS_TOTAL_COL]].max().max() * 1.1
    logging.debug(f"x axis range: {window_start} - {window_end}")
    logging.debug(f"y axis range: 0 - {window_total}")

    plot = figure(
        plot_height=None, plot_width=None,
        sizing_mode="scale_both",
        x_range=(window_start, window_end),
        y_range=(0, window_total),
        x_axis_type='datetime', toolbar_location=None,
        x_axis_label="Date",
        y_axis_label="SR Count"
    )
    # Adding extra y axis
    plot.extra_y_ranges = {
        "duration": Range1d(start=0, end=plot_df[[DURATION_COL, PREVIOUS_DURATION_COL]].max().max() * 1.2)
    }
    plot.add_layout(LinearAxis(y_range_name="duration", axis_label="Duration (Days)"), 'right')

    line_2019 = plot.line(x=DATE_COL, y=PREVIOUS_DURATION_COL,
                          source=plot_df, y_range_name="duration", color="Grey", line_width=2, alpha=0.5)

    vbar_stacked = plot.vbar_stack(
        [OPENED_COL, CLOSED_COL], x=dodge(DATE_COL, 1.5e7, plot.x_range), width=2.5e7, source=plot_df,
        fill_color=["Orange", "Blue"], alpha=0.8, line_alpha=0
    )
    previous_vbar_stacked = plot.vbar_stack(
        [PREVIOUS_OPEN_COL, PREVIOUS_CLOSED_COL], x=dodge(DATE_COL, -1.5e7, plot.x_range),
        width=2.5e7, source=plot_df,
        fill_color=["Grey", "DarkGrey"], alpha=0.5, line_alpha=0
    )

    line = plot.line(x=DATE_COL, y=DURATION_COL,
                     source=plot_df, y_range_name="duration", color="Red", line_width=2)
    circle = plot.circle(x=DATE_COL, y=DURATION_COL,
                         source=plot_df, y_range_name="duration", color="Red", size=5)

    plot.xaxis.major_label_orientation = math.pi / 2
    plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")

    legend_items = (list(zip(("Opened", "Closed"), map(lambda x: [x], vbar_stacked))) +
                    [("Duration", [line, circle])] +
                    [("2019", previous_vbar_stacked)])
    legend = Legend(items=legend_items, location="center", orientation="horizontal", margin=2, padding=2)
    plot.add_layout(legend, 'below')

    tooltips = [
        (DATE_COL, f"@{DATE_COL}{{%F}}"),
        *[(col, f"@{col}{{0.0 a}}") for col in HOVER_COLS if col not in [DATE_COL]]
    ]
    hover_tool = HoverTool(tooltips=tooltips, renderers=[line], mode="vline",
                           formatters={f'@{DATE_COL}': 'datetime'})
    plot.add_tools(hover_tool)

    plot_html = file_html(plot, CDN, "Business Continuity Service Request Time Series")

    return plot_html


def write_to_minio(html, minio_filename, minio_access, minio_secret):
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, minio_filename)

        logging.debug(f"Writing out HTML to '{local_path}'")
        with open(local_path, "w") as line_plot_file:
            line_plot_file.write(html)

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

    logging.info(f"Generat[ing] plot for '{directorate_title}'")

    logging.info("Fetch[ing] SR data...")
    service_request_df = get_service_request_data(secrets["minio"]["confidential"]["access"],
                                                  secrets["minio"]["confidential"]["secret"])
    logging.info("...Fetch[ed] SR data.")

    logging.debug(f"service_request_df.shape={service_request_df.shape}")
    logging.info(f"Filter[ing] data...")
    directorate_df = (
        service_request_df.query(f"directorate == @directorate_title")
        if directorate_title != "*"
        else service_request_df
    )
    logging.info(f"...Filter[ed] data")
    logging.debug(f"directorate_df.shape={directorate_df.shape}")

    logging.info("Mung[ing] data for plotting...")
    plot_df = generate_plot_timeseries(directorate_df)
    logging.info("...Mung[ed] data")

    logging.info("Generat[ing] Plot...")
    plot_html = generate_plot(plot_df)
    logging.info("...Generat[ed] Plot")

    logging.info("Writ[ing] to Minio...")
    plot_filename = f"{directorate_file_prefix}_{PLOT_FILENAME_SUFFIX}"
    write_to_minio(plot_html, plot_filename,
                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] to Minio")
