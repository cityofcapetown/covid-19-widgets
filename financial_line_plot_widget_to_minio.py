import datetime
import json
import math
import logging
import os
import sys
import tempfile

from bokeh.embed import file_html
from bokeh.models import HoverTool, Range1d, NumeralTickFormatter, DatetimeTickFormatter
from bokeh.plotting import figure
from bokeh.resources import CDN
from db_utils import minio_utils
import pandas

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_RESTRICTED_PREFIX = "data/private/"
WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_"
INCOME_DATA_FILENAME = "income_totals.csv"
PAYMENT_DATA_FILENAME = "business_continuity_finance_payments.csv"
PLOT_FILENAME = "line_finance_plot.html"


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

    return data_df


def get_finance_data(income_keyname, payment_keyname, minio_access, minio_secret):
    income_data = get_data(INCOME_DATA_FILENAME, minio_access, minio_secret)
    payment_data = get_data(PAYMENT_DATA_FILENAME, minio_access, minio_secret)

    return income_data, payment_data


def income_data_munge(income_df):
    logging.debug("Converting timestamp...")
    income_df.DateTimestamp = pandas.to_datetime(income_df.DateTimestamp)
    income_df.set_index('DateTimestamp', inplace=True)

    logging.debug("Dropping future dates...")
    income_df.drop(
        income_df.loc[
            income_df.index.date >= pandas.Timestamp.now().date()
            ].index,
        inplace=True
    )

    return income_df


def payment_data_munge(payment_df):
    logging.debug("Converting timestamp...")
    payment_df["DateTimestamp"] = pandas.to_datetime(payment_df.Date, format="%Y%m%d")

    logging.debug("Pivoting payment DF...")
    payment_df = payment_df.pivot(
        index="DateTimestamp",
        columns="PaymentType",
        values="DailyTotal"
    ).fillna(0)

    return payment_df


def data_munge(income_df, payment_df):
    income_df = income_data_munge(income_df)
    payment_df = payment_data_munge(payment_df)

    return income_df, payment_df


def generate_plotting_datasource(income_df, payments_df):
    # Just Total Income, Total Payments and Nett Spend for now
    totals_df = pandas.DataFrame()

    logging.debug("Setting total income and payment...")
    totals_df["TotalIncome"] = income_df.sum(axis=1)
    totals_df["TotalPayments"] = payments_df["Total"]

    # Setting missing values to 0
    logging.debug("Setting missing values to 0..")
    totals_df["TotalIncome"].fillna(0, inplace=True)
    totals_df["TotalPayments"].fillna(0, inplace=True)

    # Calculating Nett Spend
    logging.debug("Calculating Nett Spend 7 day average...")
    totals_df["NettSpend"] = (-totals_df["TotalPayments"] - totals_df["TotalIncome"]).rolling(window=5).mean()

    # Setting Date
    totals_df["Date"] = totals_df.index.date

    return totals_df


def generate_plot(plot_df, start_date="2020-03-01", sast_tz='Africa/Johannesburg'):
    start_date = datetime.datetime.combine(
        pandas.Timestamp(start_date, tz=sast_tz).date(), datetime.datetime.min.time()
    )
    end_date = datetime.datetime.combine(
        pandas.Timestamp.now(tz=sast_tz).date(), datetime.datetime.min.time()
    )

    TOOLTIPS = [
        ("Date", "@Date{%F}"),
        *[(col, f"@{col}{{0.0 a}}") for col in plot_df.columns if col not in ["Date"]]
    ]
    hover_tool = HoverTool(tooltips=TOOLTIPS,
                           formatters={'@Date': 'datetime'})

    # Main plot
    line_plot = figure(
        title=None,
        width=None, height=None,
        x_axis_type='datetime', sizing_mode="scale_both",
        x_range=(start_date, end_date), y_axis_label="Amount (ZAR)",
        toolbar_location=None
    )
    line_plot.add_tools(hover_tool)

    # Adding Income
    line_plot.vbar(x="Date", top="TotalIncome", color="black", source=plot_df,
                   legend_label="Cash Income", width=5e7)
    # Adding Payments
    line_plot.vbar(x="Date", top="TotalPayments", color="red", source=plot_df,
                   legend_label="Payments", width=5e7)

    # Adding Nett Spend
    line_plot.line(x="Date", y="NettSpend", color="purple", source=plot_df,
                   legend_label="Nett Spend (7 Day Average)", line_width=3, line_dash="dashed")

    # X-axis
    line_plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")
    line_plot.xaxis.major_label_orientation = math.pi / 4

    # Legend Location
    line_plot.legend.location = "top_left"
    line_plot.legend.visible = False

    # Y-axis
    max_value = plot_df[["TotalIncome", "TotalPayments"]].abs().quantile(0.99).max()
    line_plot.y_range = Range1d(-max_value * 1.1, max_value * 1.1)
    line_plot.yaxis.formatter = NumeralTickFormatter(format="0 a")

    plot_html = file_html(line_plot, CDN, "Business Continuity Financial Time Series")

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

    logging.info("Fetch[ing] data...")
    income_data_df, payment_data_df = get_finance_data(INCOME_DATA_FILENAME, PAYMENT_DATA_FILENAME,
                                                       secrets["minio"]["edge"]["access"],
                                                       secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")

    logging.info("Mung[ing] data for plotting...")
    income_data_df, payment_data_df = data_munge(income_data_df, payment_data_df)
    logging.info("...Mung[ed] data")

    logging.info("Generat[ing] data source for plotting...")
    plot_data_df = generate_plotting_datasource(income_data_df, payment_data_df)
    logging.info("...Generat[ed] data source for plotting")

    logging.info("Generat[ing] Plot...")
    plot_html = generate_plot(plot_data_df)
    logging.info("...Generat[ed] Plot")

    logging.info("Writ[ing] to Minio...")
    write_to_minio(plot_html, PLOT_FILENAME,
                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] to Minio")
