#!/usr/bin/env python3
"""
This script compiles the vaccine rollout report and emails it out
"""

# base imports
import argparse
from collections import defaultdict
from datetime import datetime
import json
import logging
import os
import pathlib
import pprint
import sys
import tempfile
import uuid
# external imports
from db_utils import exchange_utils
from db_utils import minio_utils
from exchangelib import (DELEGATE, Account, Credentials, Configuration, NTLM, Build, Version, HTMLBody, Message,
                         FileAttachment)
import pandas
# local imports
from vaccine_rollout_plots import OUTFILE_PREFIX, VACC_PLOT_PREFIX, TIME_SERIES_PREFIX
from vaccine_rollout_plots import (COVID_BUCKET, EDGE_CLASSIFICATION, VACCINATED_REL, VACCINATED_CUMSUM, AGG_LEVEL,
                                   TOP_LEVEL, DEPARTMENT, SUBDISTRICT, BRANCH, STAFF_TYPE, RISK, minio_to_df)
from hr_bp_emailer import load_email_template

EMAILS_BUCKET = "covid-19-vaccine-emails"
RESOURCES_PATH = "resources"
AGGREGATIONS_PREFIX = "data/private/staff_vaccine/aggregations/"
TOTAL_STAFF = "total_staff"

# sequencing dataframes
VACCINATION_TOTAL_TS = "staff-vaccination-time-series.csv"
SEQ_WILLINGNESS_TOTAL = "staff-sequencing-willingness-totals.parquet"
SEQ_WILLINGNESS_BRANCH = "staff-sequencing-willingness-by-branch.parquet"
SEQ_WILLINGNESS_TYPE = "staff-sequencing-willingness-by-type.parquet"
SEQ_WILLINGNESS_RISK = "staff-sequencing-willingness-by-risk-score.parquet"

# format df cols
STAFF_SEQD = "staff_sequenced"
PERC_SEQD = "percent_staff_sequenced"
WILLING_VACC = "willing_to_vaccinate"
PERC_WILLING_VACC = 'percent_willing_to_vaccinate'
WILLING_PILOT = "willing_for_sisonke"
PERC_WILLING_PILOT = "percent_willing_for_sisonke"
AGG_LEVEL = AGG_LEVEL.replace("_", " ")
FORMAT_INT_COLS = [STAFF_SEQD, WILLING_VACC, WILLING_PILOT]
FORMAT_PERC_COLS = [PERC_SEQD, PERC_WILLING_VACC, PERC_WILLING_PILOT]

ACCOUNT_NAME = "opm.data"
EMAIL_TEMPLATE_FILENAME = "vaccine_report_email_template.html"
CITY_LOGO_FILENAME = "rect_city_logo.png"
ISO8601_DATE_FORMAT = "%Y-%m-%d"

DATASCIENCE_CREW = ["colinscott.anthony@capetown.gov.za",
                    #                     "gordon.inggs@capetown.gov.za",
                    #                     "riaz.arbi@capetown.gov.za",
                    #                     "DelynoJohannes.DuToit@capetown.gov.za"
                    ]

vaccine_email_list = [
    ("Paul Nkurunziza", "Paul.Nkurunziza@capetown.gov.za"),
    ("Natacha Berkowitz", "Natacha.Berkowitz@capetown.gov.za"),
    ("Christa Hugo", "Christa.Hugo@capetown.gov.za"),
    ("Ruberto Isaaks", "Ruberto.Isaaks@capetown.gov.za"),
    ("Vera Scott", "Vera.Scott@capetown.gov.za"),
    ("Soraya Elloker", "Soraya.Elloker@capetown.gov.za"),
    ("Andile Zimba", "Andile.Zimba@capetown.gov.za"),
    ("Kevin Lee", "Kevin.Lee@capetown.gov.za"),
    ("Theda De Villiers", "Theda.DeVilliers@capetown.gov.za"),
    ("Babalwa Nkasana", "Babalwa.Qukula@capetown.gov.za"),
    ("Stephanie Sirmongpong", "Stephanie.Sirmongpong@capetown.gov.za"),
    ("Nomsa Nqana", "Nomsa.Nqana@capetown.gov.za"),
    ("Everin Van Rooyen", "Everin.VanRooyen@capetown.gov.za"),
    ("Kelebogile Shuping", "Kelebogile.Shuping@capetown.gov.za"),
    ("Melissa Stanley", "Melissa.Stanley@capetown.gov.za"),
    ("Mohamed Barday", "Mohamed.Barday@capetown.gov.za"),
    ("Alison Sinclair", "Alison.Sinclair@capetown.gov.za"),
    ("Jennifer Coetzee", "Jennifer.Coetzee@capetown.gov.za"),
    ("Eldre Foot", "Eldre.Foot@capetown.gov.za"),
    ("Nazeem Adams", "Nazeem.Adams@capetown.gov.za"),
    ("Florence Groener", "Florence.Groener@capetown.gov.za"),
    ("Brynt Cloete", "BryntLindsay.Cloete@capetown.gov.za"),
]

# email_group = {
#     "receiver_name": [name for name, email in vaccine_email_list],
#     "receiver_email": [email for name, email in vaccine_email_list],
#     "cc_email": DATASCIENCE_CREW,
# }

email_group = {
    "receiver_name": ["Colin"],
    "receiver_email": DATASCIENCE_CREW,
    "cc_email": DATASCIENCE_CREW,
}

EXCHANGE_VERSION = Version(build=Build(15, 0, 1395, 4000))
HTML_TABLE_PRETTY_FORMAT = 'blue_light'


def render_email(
        email_template, receiver_dict, total_vaccinated, total_staff,
        percent_vaccinated, staff_by_totals_file, staff_by_department_file, staff_by_subdistrict_file,
        staff_by_type_file, staff_willing_by_risk_file, sequencing_totals_df,
        sequencing_branch_df, sequencing_type_df, sequencing_risk_df, report_date
):
    receiver_name = receiver_dict["receiver_name"]
    receiver_name_string = receiver_name[0]

    sequencing_totals_df_html = sequencing_totals_df.to_html()
    sequencing_branch_df_html = sequencing_branch_df.to_html()
    sequencing_type_df_html = sequencing_type_df.to_html()
    sequencing_risk_df_html = sequencing_risk_df.to_html()

    # Handling any middle receivers
    if len(receiver_name) > 2:
        receiver_name_string = receiver_name_string + ", " + ", ".join(receiver_name[1:-1])

    # Handling the salutation end
    if len(receiver_name) > 1:
        receiver_name_string = receiver_name_string + f" and {receiver_name[-1]}"

    now = datetime.now()
    iso8601_timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f%Z")

    message_id = str(uuid.uuid4())

    body_dict = dict(
        receiver_name=receiver_name_string,
        report_date=report_date,
        iso8601_timestamp=iso8601_timestamp,
        request_id=message_id,

        total_vaccinated=total_vaccinated,
        total_staff=total_staff,
        percent_vaccinated=percent_vaccinated,

        staff_by_totals_file=staff_by_totals_file,
        staff_by_department_file=staff_by_department_file,
        staff_by_subdistrict_file=staff_by_subdistrict_file,
        staff_by_type_file=staff_by_type_file,
        staff_willing_by_risk_file=staff_willing_by_risk_file,

        sequencing_totals_df=sequencing_totals_df_html,
        sequencing_branch_df=sequencing_branch_df_html,
        sequencing_type_df=sequencing_type_df_html,
        sequencing_risk_df=sequencing_risk_df_html,
    )
    logging.debug(f"template_dict=\n{pprint.pformat(body_dict)}")
    body = email_template.render(**body_dict)

    subject = f"CT Metro Staff Vaccine Rollout Report: {report_date}"
    message_dict = dict(subject=subject,
                        body=HTMLBody(body),
                        to_recipients=receiver_dict["receiver_email"],
                        cc_recipients=receiver_dict["cc_email"],
                        reply_to=DATASCIENCE_CREW, )
    return (message_id, message_dict)


def send_email(account, email_message_dict, attachments, message_uuid, dry_run):
    logging.debug(f"Saving {message_uuid}.json to Minio")
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, f"{message_uuid}.json")
        with open(local_path, "w") as message_file:
            json.dump(email_message_dict, message_file)

        minio_utils.file_to_minio(
            filename=local_path,
            minio_bucket=EMAILS_BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=EDGE_CLASSIFICATION,
        )

    message = Message(account=account, **email_message_dict)

    logging.debug("Attaching logo")
    logo_path = os.path.join(RESOURCES_PATH, CITY_LOGO_FILENAME)
    with open(logo_path, "rb") as logo_file:
        message.attach(
            FileAttachment(name=CITY_LOGO_FILENAME, content=logo_file.read())
        )

    logging.debug(f"Attaching data files")
    for attachment_name, attachment_file in attachments:
        logging.debug(f"attachment_name='{attachment_name}'")
        with open(attachment_file, "rb") as plot_file:
            message.attach(
                FileAttachment(name=attachment_name, content=plot_file.read())
            )
    logging.debug(f"Sending {message_uuid} email")

    # Turning down various exchange loggers for the send - they're a bit noisy
    exchangelib_loggers = [
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if name.startswith("exchangelib")
    ]
    for logger in exchangelib_loggers:
        logger.setLevel(logging.INFO)

    if not dry_run:
        message.send(save_copy=True)
    else:
        logging.warning("**--dry_run flag set, hence not sending emails...**")

    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')
    # Getting date to run for
    parser = argparse.ArgumentParser(
        description="Pipeline script that emails HR Business Partners a report on workers assessed"
    )

    parser.add_argument('-r', '--report_date', required=True,
                        help='''Date for which the report should run. Should be an ISO8601 date, e.g. 2020-04-28''')

    parser.add_argument('-d', '--dry_run', required=False, default=False, action="store_true",
                        help="""Boolean flag indicating the emails *should NOT* actually be sent.""")

    args, _ = parser.parse_known_args()
    report_date = pandas.to_datetime(args.report_date, format=ISO8601_DATE_FORMAT)
    report_date_str = report_date.strftime(ISO8601_DATE_FORMAT)
    dry_run = args.dry_run

    # secrets var
    SECRETS_PATH_VAR = "SECRETS_PATH"
    # Loading secrets
    logging.info(f"Fetch[ing] secrets")
    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        logging.info("Trying to load from file")
        secrets_file = pathlib.Path("/home/jovyan/secrets/secrets.json").resolve()
        if not secrets_file.exists():
            logging.error("could not find your secrets")
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

    # get dfs for report tables
    logging.info("Fetch[ing] email plot dfs for tables")
    table_dfs = []
    for seq_vaccine_df in [VACCINATION_TOTAL_TS, SEQ_WILLINGNESS_TOTAL, SEQ_WILLINGNESS_BRANCH,
                           SEQ_WILLINGNESS_TYPE, SEQ_WILLINGNESS_RISK]:
        if seq_vaccine_df == VACCINATION_TOTAL_TS:
            bucket_prefix = TIME_SERIES_PREFIX
            reader = "csv"
        else:
            bucket_prefix = AGGREGATIONS_PREFIX
            reader = "parquet"
        df = minio_to_df(
            minio_filename_override=f"{bucket_prefix}{seq_vaccine_df}",
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=reader
        )
        df.rename(columns={col: col.replace("_", " ") for col in df.columns}, inplace=True)

        for col in df.columns.to_list():
            if "percent" in col:
                df[col] = round(df[col] * 100, 3)

        table_dfs.append(df)
    vaccine_df, sequencing_totals_df, sequencing_branch_df, sequencing_type_df, sequencing_risk_df = table_dfs
    vaccine_df = vaccine_df.query(f"`{AGG_LEVEL}` == @TOP_LEVEL").copy()
    logging.info("Fetch[ed] email plot dfs for tables")

    logging.info("Fetch[ing] current totals from dfs")
    vaccinated_total = vaccine_df[VACCINATED_CUMSUM.replace("_", " ")].to_list()[-1]
    staff_total = vaccine_df[TOTAL_STAFF.replace("_", " ")].to_list()[0]
    perc_vaccinated_total = round(vaccine_df[VACCINATED_REL.replace("_", " ")].to_list()[-1] * 100, 4)
    logging.info("Fetch[ed] current totals from dfs")

    # get plot files
    logging.info("Fetch[ing] email plot files")
    attachments_file_paths_dict = defaultdict(str)
    attachment_zip = []
    with tempfile.TemporaryDirectory() as temp_dir:
        for plot_level in [TOP_LEVEL, DEPARTMENT, SUBDISTRICT, STAFF_TYPE, RISK]:
            plot_file = f"{OUTFILE_PREFIX}_{plot_level}.png"
            minio_filename_override = f"{VACC_PLOT_PREFIX}{plot_file}"
            tmp_file_name = str(pathlib.Path(temp_dir, plot_file))
            minio_result = minio_utils.minio_to_file(
                filename=tmp_file_name,
                minio_filename_override=minio_filename_override,
                minio_bucket=COVID_BUCKET,
                data_classification=EDGE_CLASSIFICATION,
            )
            if not minio_result:
                logging.debug(f"Could not get data from minio bucket")
                sys.exit(-1)

            attachments_file_paths_dict[plot_level] = plot_file
            attachment_zip.append((plot_file, tmp_file_name))

        logging.info("Fetch[ed] email plot files")

        # set email params
        logging.info("Load[ing] email template")
        email_template_path = os.path.join(RESOURCES_PATH, EMAIL_TEMPLATE_FILENAME)
        email_template = load_email_template(email_template_path)
        logging.info("Load[ed] email template")

        logging.info("Generat[ing] email html")
        message_id, email_message_dict = render_email(
            email_template,
            receiver_dict=email_group,

            total_vaccinated=vaccinated_total,
            total_staff=staff_total,
            percent_vaccinated=perc_vaccinated_total,

            staff_by_totals_file=attachments_file_paths_dict[TOP_LEVEL],
            staff_by_department_file=attachments_file_paths_dict[DEPARTMENT],
            staff_by_subdistrict_file=attachments_file_paths_dict[SUBDISTRICT],
            staff_by_type_file=attachments_file_paths_dict[STAFF_TYPE],
            staff_willing_by_risk_file=attachments_file_paths_dict[RISK],

            sequencing_totals_df=sequencing_totals_df,
            sequencing_branch_df=sequencing_branch_df,
            sequencing_type_df=sequencing_type_df,
            sequencing_risk_df=sequencing_risk_df,

            report_date=report_date_str
        )
        logging.info("Generat[ed] email html")

        logging.info("Connect[ing] to email account")
        account = exchange_utils.setup_exchange_account(
            username=secrets["proxy"]["username"],
            password=secrets["proxy"]["password"],
        )
        logging.info("Connect[ed] to email account")

        logging.info("Send[ing] email")
        result = send_email(
            account,
            email_message_dict,
            attachment_zip,
            message_id,
            dry_run=dry_run
        )
        logging.info("Sen[t] email")

        logging.info("Done")
