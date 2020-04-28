import json
import logging
import os
import pprint
import sys
import tempfile

from bokeh.embed import file_html
from bokeh.models import HoverTool, Range1d, ColumnDataSource,  LinearAxis, Span, Label, DatetimeTickFormatter
from bokeh.models.widgets.tables import HTMLTemplateFormatter, TableColumn, DataTable, DateFormatter

from bokeh.plotting import figure
from bokeh.resources import CDN
from db_utils import minio_utils
import pandas

MINIO_BUCKET = "covid"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

DATA_RESTRICTED_PREFIX = "data/private/"
MEDIA_DATA_FILENAME = "media_complete.csv"
DATE_COL_NAME = "published"

START_DATE = "2020-03-01"
TZ_STRING = "Africa/Johannesburg"
END_DATE = pandas.Timestamp.now(tz=TZ_STRING).strftime("%Y-%m-%d")
ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M"

MEDIA_SOURCES = ('Press', 'Twitter', 'Facebook')
MEDIA_SOURCES_CLASSIFICATION = (("PRESS", "NA"),
                                ("CONSUMER", "TWITTER"),
                                ("CONSUMER", "FACEBOOK"))
COLOURS = ("#fc8d59", "#ffffbf", "#91bfdb")
SOURCE_COLOURS = ("#b8b9bb", "#1da1f2", "#4267b2")
LONG_TERM_SENTIMENT = -52.2

WIDGETS_RESTRICTED_PREFIX = "widgets/private/behavioural_"
OUTPUT_VALUE_FILENAME = "values.json"
MEDIA_COUNT_PLOT_FILENAME = "media_count_plot.html"
SENTIMENT_TS_PLOT_FILENAME = "sentiment_ts_plot.html"
MOST_RECENT_MEDIA_TABLE = "most_recent_media_table.html"
MOST_RECENT_MEDIA_TABLE_JSON = "most_recent_media_table.json"
POSITIVE_COMMENTS_TABLE = "positive_comments_table.html"
POSITIVE_COMMENTS_TABLE_JSON = "positive_comments_table.json"
NEGATIVE_COMMENTS_TABLE = "negative_comments_table.html"
NEGATIVE_COMMENTS_TABLE_JSON = "negative_comments_table.json"


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


def get_latest_values_dict(mentions_df):
    last_updated = mentions_df.published.max().strftime(ISO_TIMESTAMP_FORMAT)
    mentions = mentions_df.shape[0]
    nett_sentiment = (
            (mentions_df.sentiment.sum() / mentions_df.sentiment.count()) * 100
    ).round(2)

    behaviour_dict = {
        "last_updated": last_updated,
        "mentions": mentions,
        "nett_sentiment": nett_sentiment
    }

    return behaviour_dict


def to_json_data(values_dict):
    json_data = json.dumps(values_dict)

    return json_data


def get_published_count(mentions_df, category_id, social_media_network_id, start_date, end_date):
    start_timestamp = pandas.to_datetime(start_date).tz_localize('Africa/Johannesburg')
    filtered_mentions = mentions_df.query(
        "categoryId == @category_id & "
        "socialNetworkId == @social_media_network_id &"
        "published >= @start_timestamp"
    ).set_index("published").resample(
        "1D",
    ).count()["id"].rename("Count")

    empty_series = pandas.Series(
        0, index=pandas.date_range(start_date, end_date, tz=TZ_STRING)
    )
    filled_series = filtered_mentions.combine_first(empty_series)

    return filled_series.sort_index()


def get_media_count_datasource(mentions_df):
    time_data = {
        "date": pandas.date_range(START_DATE, END_DATE, tz=TZ_STRING),
        **{media_source: get_published_count(mentions_df, category_id, smn_id, START_DATE, END_DATE).values
           for media_source, (category_id, smn_id) in zip(MEDIA_SOURCES, MEDIA_SOURCES_CLASSIFICATION)},
    }

    return time_data


def generate_media_count_plot(media_count_datasource):
    tooltips = [
        ("Date", "@date{%F}"),
        *[(media_source, f"@{media_source}") for media_source in MEDIA_SOURCES]
    ]
    hover_tool = HoverTool(tooltips=tooltips,
                           formatters={'@date': 'datetime'})

    line_plot = figure(
        title=None,
        width=None, height=None,
        x_range=(media_count_datasource["date"].min(), media_count_datasource["date"].max()),
        toolbar_location=None, sizing_mode="scale_both", x_axis_type='datetime',
        tools=[hover_tool]
    )

    for media_source, media_source_colour in zip(MEDIA_SOURCES, SOURCE_COLOURS):
        line_plot.line(x="date", y=media_source, color=media_source_colour, source=media_count_datasource,
                       legend_label=media_source, line_width=5)

    line_plot.legend.location = "top_left"
    line_plot.y_range.start = 0
    line_plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")

    plot_html = file_html(line_plot, CDN, "Behavioural Media Channel Timeseries")

    return plot_html


def get_nett_sentiment(mentions_df, start_date, end_date):
    # Filtering and resampling to per day
    start_timestamp = pandas.to_datetime(start_date).tz_localize('Africa/Johannesburg')
    filtered_mentions = (
        mentions_df.query("published >= @start_timestamp")
    ).set_index("published").resample(
        "1D",
    )

    # calculating the nett sentiment per day
    sentiment_sum = filtered_mentions.sum()["sentiment"].rename("Sum")
    sentiment_abs_count = filtered_mentions.agg(
        lambda series: series.count()
    )["sentiment"].rename("AbsCount")
    sentiment_series = (sentiment_sum / sentiment_abs_count * 100).round(2)

    # assigning the values to the specified time range
    empty_series = pandas.Series(
        0, index=pandas.date_range(start_date, end_date, tz=TZ_STRING)
    )
    filled_series = sentiment_series.combine_first(empty_series)

    return filled_series.sort_index(), sentiment_abs_count


def get_sentiment_ts_datasource(mentions_df):
    sentiment_values, sentiment_sample_count = get_nett_sentiment(mentions_df, START_DATE, END_DATE)

    time_sentiment_data = {
        "date": pandas.date_range(START_DATE, END_DATE, tz=TZ_STRING),
        "NettSentiment": sentiment_values,
        "Count": sentiment_sample_count
    }

    return time_sentiment_data, sentiment_sample_count


def generate_sentiment_ts_plot(sentiment_ts_data, sentiment_ts_sample_count):
    tooltips = [
        ("Date", "@date{%F}"),
        ("Mentions", "@Count"),
        ("Nett Sentiment", "@NettSentiment %"),
    ]

    line_plot = figure(title=None,
                       width=None, height=None,
                       sizing_mode="scale_both",
                       x_range=(sentiment_ts_data["date"].min(), sentiment_ts_data["date"].max()),
                       x_axis_type='datetime',
                       y_axis_label="Nett Sentiment (%)",
                       tools=[], toolbar_location=None
                       )
    # Setting range of y range
    line_plot.y_range = Range1d(-100, 100)

    # Adding Count range on the right
    line_plot.extra_y_ranges = {"count_range": Range1d(start=0, end=sentiment_ts_sample_count.max() * 1.1)}
    line_plot.add_layout(LinearAxis(y_range_name="count_range", axis_label="Mentions"), 'right')

    sentiment_count_bars = line_plot.vbar(x="date", top="Count", width=5e7, color="orange", source=sentiment_ts_data,
                                          y_range_name="count_range")
    sentiment_line = line_plot.line(x="date", y="NettSentiment", color="blue", source=sentiment_ts_data, line_width=5)

    # Long Term sentiment baseline
    long_term_sentiment_span = Span(location=LONG_TERM_SENTIMENT, name="LongTermSentiment",
                                    dimension='width', line_color='red',
                                    line_dash='dashed', line_width=3, )
    line_plot.add_layout(long_term_sentiment_span)

    start_timestamp = sentiment_ts_data["date"].min()
    long_term_sentiment_label = Label(x=start_timestamp, y=LONG_TERM_SENTIMENT, x_offset=-10, y_offset=10,
                                      text='Sentiment Baseline', render_mode='css',
                                      border_line_color='white', border_line_alpha=0.0, angle=0, angle_units='deg',
                                      background_fill_color='white', background_fill_alpha=0.0, text_color='red',
                                      text_font_size='12pt')
    line_plot.add_layout(long_term_sentiment_label)

    hover_tool = HoverTool(renderers=[sentiment_line, sentiment_count_bars], tooltips=tooltips,
                           formatters={'@date': 'datetime', 'NettSentiment': 'printf'})
    line_plot.add_tools(hover_tool)

    line_plot.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")

    plot_html = file_html(line_plot, CDN, "Behavioural Sentiment Timeseries")

    return plot_html


def get_most_recent_media_datasource(mentions_df):
    most_recent = mentions_df.query("title.notna()")[
        ["published", "title", "link"]
    ].sort_values(["published"], ascending=False).copy()

    media_data_source = ColumnDataSource(most_recent)

    most_recent_export = most_recent.copy().assign(
        **{DATE_COL_NAME + "_str": most_recent[DATE_COL_NAME].dt.strftime(ISO_TIMESTAMP_FORMAT)}
    ).drop(
        [DATE_COL_NAME], axis='columns'
    )
    media_data_source_json = most_recent_export.to_json(orient='records')

    return media_data_source, media_data_source_json


def get_table_formatters():
    datetime_formatter = DateFormatter(format="%Y-%m-%dT%H:%M")
    title_link_template = '''
          <table style="table-layout: fixed; width: 100%;white-space: normal;">
            <tr>
              <td style="word-wrap: break-word;text-align:left">
                <a href="<%= link %>"><%= value %></a>
              </td>
            </tr>
          </table>
        '''
    title_link_formatter = HTMLTemplateFormatter(template=title_link_template)

    return datetime_formatter, title_link_formatter


def generate_most_recent_media_table(most_media_datasource):
    datetime_formatter, title_link_formatter = get_table_formatters()

    media_data_columns = [
        TableColumn(field="published", title="Date Published", width=105, formatter=datetime_formatter),
        TableColumn(field="title", title="Media Links", formatter=title_link_formatter, width=1000 - 105),
    ]

    media_data_table = DataTable(source=most_media_datasource,
                                 columns=media_data_columns,
                                 fit_columns=False, selectable=False,
                                 sortable=True, index_position=None,
                                 height=300, width=1000,
                                 sizing_mode="scale_both")

    table_html = file_html(media_data_table, CDN, "Most Recent Media Links")

    return table_html


def get_social_media_comments_datasource(mentions_df, sentiment_value='-1'):
    extracts = mentions_df.query(
        "extract.notna() & sentiment == @sentiment_value & socialNetworkId != 'NA'"
    )[
        ["published", "extract", "link", "engagement", "authorId"]
    ].sort_values("engagement", ascending=False).copy()

    data_source = ColumnDataSource(extracts)

    extracts_export = extracts.copy().assign(
        **{DATE_COL_NAME + "_str": extracts[DATE_COL_NAME].dt.strftime(ISO_TIMESTAMP_FORMAT)}
    ).drop(
        [DATE_COL_NAME], axis='columns'
    )
    extracts_json = extracts_export.to_json(orient='records')

    return data_source, extracts_json


def generate_social_media_comments_table(media_comments_data_source, sentiment_label="Negative"):
    datetime_formatter, title_link_formatter = get_table_formatters()
    data_columns = [
        TableColumn(field="published", title="Date Published", width=105, formatter=datetime_formatter,
                    name="time_series"),
        TableColumn(field="extract", title=f"{sentiment_label} Covid-19 Facebook Comments by Engagement",
                    formatter=title_link_formatter, name="extract",
                    width=380)
    ]

    data_table = DataTable(source=media_comments_data_source, columns=data_columns,
                           fit_columns=False, selectable=False,
                           sortable=True, index_position=None,
                           width=500, row_height=100, )

    table_html = file_html(data_table, CDN, f"Most Engaged {sentiment_label} Social Media Comments")

    return table_html


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
    mentions_data_df = get_data(MEDIA_DATA_FILENAME,
                                secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Fetch[ed] data.")

    logging.info("Generat[ing] latest values...")
    latest_values = get_latest_values_dict(mentions_data_df)
    latest_values_json = to_json_data(latest_values)
    logging.info("...Generat[ed] latest values")

    logging.info("Generat[ing] media count values for plot...")
    media_count_source = get_media_count_datasource(mentions_data_df)
    logging.info("...Generat[ed] media count values for plot")

    logging.info("Generat[ing] media count plot...")
    media_count_plot_html = generate_media_count_plot(media_count_source)
    logging.info("...Generat[ed] media count plot")

    logging.info("Generat[ing] sentiment time series values for plot...")
    sentiment_ts_source, sentiment_ts_samples = get_sentiment_ts_datasource(mentions_data_df)
    logging.info("...Generat[ed] sentiment time series values for plot")

    logging.info("Generat[ing] sentiment time series plot...")
    sentiment_ts_plot_html = generate_sentiment_ts_plot(sentiment_ts_source, sentiment_ts_samples)
    logging.info("...Generat[ed] sentiment time series plot")

    logging.info("Generat[ing] media links for table...")
    media_table_source, media_table_json = get_most_recent_media_datasource(mentions_data_df)
    logging.info("...Generat[ed] media links for table")

    logging.info("Generat[ing] media links table...")
    media_table_html = generate_most_recent_media_table(media_table_source)
    logging.info("...Generat[ed] media links table")

    extracts_table_json = {}
    extracts_table_html = {}
    for sentiment_label, sentiment_value in (('Positive', '1'),
                                             ('Negative', '-1')):
        logging.info(f"Generat[ing] {sentiment_label} extracts for table...")
        extracts_source, extracts_json = get_social_media_comments_datasource(mentions_data_df, sentiment_value)
        extracts_table_json[sentiment_label] = extracts_json
        logging.info(f"...Generat[ed] {sentiment_label} extracts for table")

        logging.info("Generat[ing] media links table...")
        extracts_table_html[sentiment_label] = generate_social_media_comments_table(extracts_source, sentiment_label)
        logging.info("...Generat[ed] media links table")

    logging.info("Writ[ing] everything to Minio...")
    for content, filename in (
            (latest_values_json, OUTPUT_VALUE_FILENAME),
            (media_count_plot_html, MEDIA_COUNT_PLOT_FILENAME),
            (sentiment_ts_plot_html, SENTIMENT_TS_PLOT_FILENAME),
            (media_table_html, MOST_RECENT_MEDIA_TABLE),
            (media_table_json, MOST_RECENT_MEDIA_TABLE_JSON),
            (extracts_table_html["Positive"], POSITIVE_COMMENTS_TABLE),
            (extracts_table_json["Positive"], POSITIVE_COMMENTS_TABLE_JSON),
            (extracts_table_html["Negative"], NEGATIVE_COMMENTS_TABLE),
            (extracts_table_json["Negative"], NEGATIVE_COMMENTS_TABLE_JSON),
    ):
        write_to_minio(content, filename,
                       secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("...Wr[ote] everything to Minio")

    logging.info("...Done!")
