#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import os
import pprint
import sys
import tempfile
import uuid

from db_utils import minio_utils
from exchangelib import DELEGATE, Account, Credentials, Configuration, NTLM, Build, Version, HTMLBody, Message, \
    FileAttachment
import holidays
import jinja2
import pandas

from hr_data_last_values_to_minio import directorate_filter_df

BUCKET = "covid"
CLASSIFICATION = minio_utils.DataClassification.EDGE
HR_TRANSACTIONAL_FILENAME_PATH = "data/private/business_continuity_people_status.csv"
HR_MASTER_FILENAME_PATH = "data/private/city_people.csv"
HR_ORG_UNIT_STATUSES = "data/private/business_continuity_org_unit_statuses.csv"

PRESENT_EMPLOYEES_SHEETNAME = "Assessed"
MISSING_EMPLOYEES_SHEETNAME = "Not Assessed"

HR_STAFFNUMBER = "StaffNumber"
DATE_COL = "Date"
DIRECTORATE_COL = "Directorate"
CATEGORY_COL = "Categories"
EVALUATION_COL = "Evaluation"
ORG_HIERACHY = ["Directorate", "Department", "Branch", "Section",]
ESSENTIAL_COL = "EssentialStaff"
ASSESSED_COL = "AssessedStaff"
APPROVER_COL = "Approver"
APPROVER_STAFF_NO_COL = "ApproverStaffNumber"
HR_PEOPLE_SHARE_COLS = [HR_STAFFNUMBER, DATE_COL, "Position", 'Employee', "Org Unit Name",
                        APPROVER_COL, APPROVER_STAFF_NO_COL,
                        CATEGORY_COL, EVALUATION_COL,
                        *ORG_HIERACHY,
                        "FebMostCommonClockingLocation", ]
APPROVER_MASTER_COL = "Approver Name"
APPROVER_MASTER_STAFF_NO_COL = "Approver Staff No"
HR_MISSING_PEOPLE_SHARE_COLS = [HR_STAFFNUMBER, "Position", 'Employee', "Org Unit Name",
                                APPROVER_MASTER_COL, APPROVER_MASTER_STAFF_NO_COL,
                                *ORG_HIERACHY,
                                "FebMostCommonClockingLocation"]
COUNT_COL = "StatusCount"
HR_ORG_UNIT_SHARE_COLS = [
    "Org Unit Name", *ORG_HIERACHY, DATE_COL, EVALUATION_COL,
]
TOTAL_COUNT = "Total"

EMAIL_SUBJECT_TEMPLATE = "{} Directorate HR Capacity Report - {}"
ISO8601_DATE_FORMAT = "%Y-%m-%d"

RESOURCES_PATH = "resources"
EMAIL_TEMPLATE_FILENAME = "hr_bp_staff_report_email_template.html"
CITY_LOGO_FILENAME = "rect_city_logo.png"

# ToDo Move to config file
DATASCIENCE_CREW = ["gordon.inggs@capetown.gov.za",
                    "riaz.arbi@capetown.gov.za",
                    "DelynoJohannes.DuToit@capetown.gov.za"]
HR_CREW = ["Joanne.Haasbroek@capetown.gov.za",
           "CarolAnne.Wright@capetown.gov.za",
           "Lucia.VanDerMerwe@capetown.gov.za",
           "Rudolph.Pollard@capetown.gov.za"]
DIRECTORATE_DETAILS_DICT = {
    # "WATER AND WASTE":
    #     {"receiver_name": ["Deon"],
    #      "receiver_email": ["Deon.Franks@capetown.gov.za"],
    #      "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    # "COMMUNITY SERVICES and HEALTH":
    #     {"receiver_name": ["Hennie"],
    #      "receiver_email": ["Hendrik.Viviers@capetown.gov.za"],
    #      "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    # "SAFETY AND SECURITY":
    #     {"receiver_name": ["Althea"],
    #      "receiver_email": ["Althea.Daniels@capetown.gov.za"],
    #      "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    # "ENERGY AND CLIMATE CHANGE":
    #     {"receiver_name": ["Maurietta"],
    #      "receiver_email": ["Maurietta.Page@capetown.gov.za"],
    #      "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    "FINANCE":
        {"receiver_name": ["Tembekile", "Louise", "Petro", "Bertie"],
         "receiver_email": ["Tembekile.Solanga@capetown.gov.za", "Louise.Muller@capetown.gov.za", "Petro.Rheeder@capetown.gov.za", "Bertie.vanNiekerk@capetown.gov.za"],
         "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    # "CORPORATE SERVICES":
    #     {"receiver_name": ["Gordon"],
    #      "receiver_email": ["gordon.inggs@capetown.gov.za"],
    #      "cc_email": ["gordon.inggs@capetown.gov.za"]},
    "CORPORATE SERVICES":
        {"receiver_name": ["Phindile", "Ashwin"],
         "receiver_email": ["PreciousPhindile.Dlamini@capetown.gov.za", "Ashwin.Martin@capetown.gov.za"],
         "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    "TRANSPORT":
        {"receiver_name": ["Austin", "Dawie", "Andrea", "Andre"],
         "receiver_email": ["Austin.Joemat@capetown.gov.za", "DavidRaimund.Bosch@capetown.gov.za", "AndreaBenedicta.DeUjfalussy@capetown.gov.za", "Andre.Maxwell@capetown.gov.za"],
         "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    "ECONOMIC OPPORTUNITIES &ASSET MANAGEMENT":
        {"receiver_name": ["Roline"],
         "receiver_email": ["Roline.Henning@capetown.gov.za"],
         "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    "SPATIAL PLANNING AND ENVIRONMENT":
        {"receiver_name": ["Leonie"],
         "receiver_email": ["Leonie.Kroese@capetown.gov.za"],
         "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    "HUMAN SETTLEMENTS":
        {"receiver_name": ["Gerard"],
         "receiver_email": ["Gerard.Joyce@capetown.gov.za"],
         "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    # "HUMAN SETTLEMENTS":
    #     {"receiver_name": ["Gordon"],
    #      "receiver_email": ["gordon.inggs@capetown.gov.za"],
    #      "cc_email": []},
    "URBAN MANAGEMENT":
        {"receiver_name": ["Sibusiso", "Neliswa"],
         "receiver_email": ["Sibusiso.Mayekiso@capetown.gov.za", "Neliswa.Gaji@capetown.gov.za"],
         "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
    "CITY MANAGER":
        {"receiver_name": ["Phindile", "Ashwin"],
         "receiver_email": ["PreciousPhindile.Dlamini@capetown.gov.za", "Ashwin.Martin@capetown.gov.za"],
         "cc_email": HR_CREW + ["Lele.Sithole@capetown.gov.za"]},
}

EXCHANGE_VERSION = Version(build=Build(15, 0, 1395, 4000))


def get_data_df(filename, minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_data_file:
        logging.debug("Pulling data from Minio bucket...")
        result = minio_utils.minio_to_file(
            temp_data_file.name,
            BUCKET,
            minio_access,
            minio_secret,
            CLASSIFICATION,
            minio_filename_override=filename
        )
        assert result

        logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
        data_df = pandas.read_csv(temp_data_file)

    return data_df


def get_exchange_auth(username, password):
    # Setting up Exchange Creds
    credentials = Credentials(username=username, password=password)

    config = Configuration(
        server="webmail.capetown.gov.za",
        credentials=credentials,
        version=EXCHANGE_VERSION,
        auth_type=NTLM
    )
    account = Account(
        primary_smtp_address="opm.data@capetown.gov.za",
        config=config, autodiscover=False,
        access_type=DELEGATE
    )

    return account


def get_today_directorate_df(data_df, directorate, filter_date=None, date_filter=True):
    logging.debug(f"data_df.head(5)=\n{data_df.head(5)}")
    logging.debug(f"directorate={directorate}, filter_date={filter_date}")

    query_df = directorate_filter_df(data_df, directorate)
    if date_filter:
        query_df = query_df.loc[
            data_df[DATE_COL] == filter_date
        ]

    logging.debug(f"query_df.head(5)=\n{query_df.head(5)}")

    return query_df


def get_pivot_df(unit_status_df):
    logging.debug(f"unit_status_df.iloc[0]=\n{unit_status_df.iloc[0]}")

    # Ugly cleaning that needs to be done temporarily
    if "Unnamed: 0" in unit_status_df.columns:
        unit_status_df.drop("Unnamed: 0", axis='columns', inplace=True)

    non_pivot_cols = [col for col in unit_status_df.columns if col not in (CATEGORY_COL, COUNT_COL)]
    unit_status_df.loc[:, non_pivot_cols] = unit_status_df.loc[:, non_pivot_cols].fillna("N/A")
    pivot_df = unit_status_df.pivot_table(
        index=non_pivot_cols,
        columns=CATEGORY_COL, values=COUNT_COL,
        fill_value=0, aggfunc="sum", margins=True, margins_name=TOTAL_COUNT,
    )
    logging.debug(f"pivot_df.head(5)=\n{pivot_df.head(5)}")
    sorted_pivot_df = pivot_df.sort_values(by=[*ORG_HIERACHY, TOTAL_COUNT], ascending=False)
    logging.debug(f"sorted_pivot_df.head(5)=\n{sorted_pivot_df.head(5)}")

    return pivot_df.reset_index()


def merge_df(hr_df, hr_master_df):
    combined_df = hr_df.merge(
        hr_master_df,
        left_on=HR_STAFFNUMBER,
        right_on=HR_STAFFNUMBER,
        how='left',
        validate="many_to_one",
    )
    logging.debug(f"combined_df.head(5)=\n{combined_df.head(5)}")

    return combined_df


def get_missing_workers_df(directorate_merged_df, directorate_master_df):
    missing_df = directorate_master_df.loc[
        directorate_master_df[ASSESSED_COL] &
        ~directorate_master_df[HR_STAFFNUMBER].isin(directorate_merged_df[HR_STAFFNUMBER])
        ]
    logging.debug(f"missing_df.head(5)=\n{missing_df.head(5)}")
    logging.debug(f"missing_df.columns=\n{missing_df.columns}")

    missing_df.sort_values([APPROVER_MASTER_COL, *ORG_HIERACHY], inplace=True)

    return missing_df


def get_email_stats(directorate_merged_df, directorate_master_df):
    assessed_workers = directorate_merged_df[ASSESSED_COL].sum()
    missing_workers = get_missing_workers_df(directorate_merged_df, directorate_master_df)

    assessed_counts = directorate_merged_df[APPROVER_STAFF_NO_COL].value_counts()
    not_assessed_counts = missing_workers[APPROVER_MASTER_STAFF_NO_COL].value_counts()

    # Forming approver stats table
    approver_stats_df = pandas.DataFrame([
        assessed_counts.rename("Assessed").astype(int),
        not_assessed_counts.rename("Not Assessed").astype(int),
    ]).transpose().fillna(0)
    approver_stats_df["Total"] = approver_stats_df.sum(axis=1)

    approver_stats_df.index = approver_stats_df.index.astype(int).astype(str)
    logging.debug(f"approver_stats_df.head(10)=\n{approver_stats_df.head(10)}")

    # Merging in approver name and sorting
    directorate_master_df[HR_STAFFNUMBER] = directorate_master_df[HR_STAFFNUMBER].astype(str)
    logging.debug(directorate_master_df.columns)
    approver_stats_df = approver_stats_df.merge(
        directorate_master_df[['Employee', HR_STAFFNUMBER]],
        left_index=True, right_on=HR_STAFFNUMBER,
        validate="one_to_one"
    ).sort_values(
        by=["Not Assessed", "Employee"],
        ascending=False
    )
    logging.debug(f"approver_stats_df.head(10)=\n{approver_stats_df.head(10)}")

    total_assessed_workers = directorate_master_df[ASSESSED_COL].sum()

    return assessed_workers, total_assessed_workers, approver_stats_df


def write_employee_file(df_tuples):
    with tempfile.NamedTemporaryFile("rb", suffix=".xlsx") as df_tempfile:
        with pandas.ExcelWriter(df_tempfile.name) as excel_writer:
            for sheet_name, data_df in df_tuples:
                data_df.fillna("").to_excel(excel_writer, sheet_name=sheet_name, index=False)

        yield df_tempfile


def write_org_file(org_df):
    with tempfile.NamedTemporaryFile("rb", suffix=".xlsx") as df_tempfile:
        org_df.fillna("").to_excel(df_tempfile.name, index=False)

        yield df_tempfile


def load_email_template(email_filename):
    # Loading email template
    with open(email_filename, "r") as email_file:
        email_template = jinja2.Template(email_file.read())

    return email_template


def render_email(email_template, receiver_dict, directorate,
                 total_assessed_workers, total_hr_workers, approver_details_df, report_date):
    receiver_name = receiver_dict["receiver_name"]
    receiver_name_string = receiver_name[0]

    # Handling any middle receivers
    if len(receiver_name) > 2:
        receiver_name_string = receiver_name_string + ", " + ", ".join(receiver_name[1:-1])

    # Handling the salutation end
    if len(receiver_name) > 1:
        receiver_name_string = receiver_name_string + f" and {receiver_name[-1]}"

    now = datetime.datetime.now()
    iso8601_timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f%Z")

    directorate_filename = directorate.lower().replace(" ", "_")
    directorate_employee_status_filename = f"{directorate_filename}_employee_status_{report_date}.xls"
    directorate_org_unit_status_filename = f"{directorate_filename}_org_unit_status_{report_date}.xls"

    message_id = str(uuid.uuid4())

    approver_details_df = approver_details_df[approver_details_df["Not Assessed"] > 0][
        ['Employee', "StaffNumber", "Not Assessed", "Assessed", "Total"]
    ]

    body_dict = dict(
        directorate=directorate,
        receiver_name=receiver_name_string,
        iso8601_date=report_date,
        iso8601_timestamp=iso8601_timestamp,
        directorate_employee_status_filename=directorate_employee_status_filename,
        directorate_org_unit_status_filename=directorate_org_unit_status_filename,
        request_id=message_id,
        assessed_workers=total_assessed_workers,
        total_workers=total_hr_workers,
        approver_details_df=approver_details_df,
    )
    logging.debug(f"template_dict=\n{pprint.pformat(body_dict)}")
    body = email_template.render(**body_dict)

    subject = EMAIL_SUBJECT_TEMPLATE.format(directorate, report_date)
    message_dict = dict(subject=subject,
                        body=HTMLBody(body),
                        to_recipients=receiver_dict["receiver_email"],
                        cc_recipients=receiver_dict["cc_email"],
                        reply_to=DATASCIENCE_CREW,)

    return (message_id, message_dict,
            directorate_employee_status_filename,
            directorate_org_unit_status_filename)


def send_email(account, email_message_dict, attachments, message_uuid, dry_run):
    logging.debug(f"Saving {message_uuid}.json to Minio")
    with tempfile.TemporaryDirectory() as tempdir:
        local_path = os.path.join(tempdir, f"{message_uuid}.json")
        with open(local_path, "w") as message_file:
            json.dump(email_message_dict, message_file)

        minio_utils.file_to_minio(
            filename=local_path,
            minio_bucket="covid-19-dash-hr-bp-emails",
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=minio_utils.DataClassification.EDGE,
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
        attachment_content = attachment_file.read()

        logging.debug("Backing up attachment file to Minio...")
        with tempfile.TemporaryDirectory() as tempdir:
            local_path = os.path.join(tempdir, attachment_name)
            with open(local_path, "wb") as attachment_temp_file:
                attachment_temp_file.write(attachment_content)

            minio_utils.file_to_minio(
                filename=local_path,
                filename_prefix_override=message_uuid + "/",
                minio_bucket="covid-19-dash-hr-bp-emails",
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
                data_classification=minio_utils.DataClassification.EDGE,
            )

        logging.debug("Attaching the content")
        message.attach(
            FileAttachment(name=attachment_name, content=attachment_content)
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
        logging.warning("**--not-a-drill flag not set, hence not sending emails...**")

    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Getting date to run for
    parser = argparse.ArgumentParser(
        description="Pipeline script that emails HR Business Partners a report on workers assessed"
    )

    parser.add_argument('-r', '--report-date', required=True,
                        help='''Date for which the report should run. Should be an ISO8601 date, e.g. 2020-04-28''')

    parser.add_argument('-o', '--directorate', required=True,
                        help='''Directorate which this report should cover''')

    parser.add_argument('-d', '--not-a-drill', required=False, default=False, action="store_true",
                        help="""Boolean flag indicating the emails *should* actually be sent.""")

    args, _ = parser.parse_known_args()
    report_date = pandas.to_datetime(args.report_date, format="%Y-%m-%d")
    report_date_str = report_date.strftime(ISO8601_DATE_FORMAT)

    directorate = args.directorate

    dry_run = not args.not_a_drill
    logging.warning(f"**This {'is' if dry_run else '*is not*'} a drill**")

    logging.debug(f"Run args: report_date='{report_date}', dry_run='{dry_run}'")

    za_holidays = holidays.CountryHoliday("ZA")
    if report_date in za_holidays or report_date.weekday == 6:
        logging.info(
            f"'{report_date}' is a Sunday or South Africa public holiday, so exiting normally without doing anything..."
        )
        sys.exit(0)

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("Gett[ing] HR Data")
    HR_TRANSACTIONAL_FILENAME_PATH = "data/private/business_continuity_people_status.csv"
    hr_transactional_df = get_data_df(HR_TRANSACTIONAL_FILENAME_PATH,
                                      secrets["minio"]["edge"]["access"],
                                      secrets["minio"]["edge"]["secret"])
    hr_master_df = get_data_df(HR_MASTER_FILENAME_PATH,
                               secrets["minio"]["edge"]["access"],
                               secrets["minio"]["edge"]["secret"])
    hr_org_unit_df = get_data_df(HR_ORG_UNIT_STATUSES,
                                 secrets["minio"]["edge"]["access"],
                                 secrets["minio"]["edge"]["secret"])
    hr_combined_people_df = merge_df(hr_transactional_df, hr_master_df)
    logging.info("G[ot] HR Data")

    logging.info("Auth[ing] with Exchange")
    exchange_account = get_exchange_auth(secrets["proxy"]["username"],
                                         secrets["proxy"]["password"])
    logging.info("Auth[ed] with Exchange")

    logging.info("Load[ing] email template")
    email_template_path = os.path.join(RESOURCES_PATH, EMAIL_TEMPLATE_FILENAME)
    hr_email_template = load_email_template(email_template_path)
    logging.info("Load[ed] email template")

    logging.info("Generat[ing] directorate level emails")
    directorate_dict = DIRECTORATE_DETAILS_DICT[directorate]

    # Getting directorate level data
    hr_combined_people_df[DATE_COL] = pandas.to_datetime(
        hr_combined_people_df[DATE_COL], format="%Y-%m-%dT%H:%M:%S"
    ).dt.strftime(ISO8601_DATE_FORMAT)
    directorate_people_df = get_today_directorate_df(hr_combined_people_df, directorate, report_date_str)
    directorate_master_people_df = get_today_directorate_df(hr_master_df, directorate, date_filter=False)

    hr_org_unit_df[DATE_COL] = pandas.to_datetime(
        hr_org_unit_df[DATE_COL], format=ISO8601_DATE_FORMAT
    ).dt.strftime(ISO8601_DATE_FORMAT)
    directorate_org_df = get_today_directorate_df(hr_org_unit_df, directorate, report_date_str)

    if directorate_people_df.shape[0] == 0 or directorate_org_df.shape[0] == 0:
        logging.debug("Skipping because one of the DFs is empty...")
        sys.exit(0)

    # Various DF calculations
    directorate_people_df = directorate_people_df
    directorate_missing_people_df = get_missing_workers_df(directorate_people_df, directorate_master_people_df)
    directorate_org_pivot_df = get_pivot_df(directorate_org_df)

    assessed_workers, total_workers, approver_details = get_email_stats(directorate_people_df,
                                                                        directorate_master_people_df)

    # Attachment file generator
    directorate_files = (
        filename
        for data_files in (
            write_employee_file((
                (PRESENT_EMPLOYEES_SHEETNAME, directorate_people_df[HR_PEOPLE_SHARE_COLS]),
                (MISSING_EMPLOYEES_SHEETNAME, directorate_missing_people_df[HR_MISSING_PEOPLE_SHARE_COLS])
            )),
            write_org_file(directorate_org_pivot_df)
        )
        for filename in data_files
    )

    # Rendering email

    message_id, email_message_dict, *data_filenames = render_email(hr_email_template,
                                                                   directorate_dict,
                                                                   directorate,
                                                                   assessed_workers,
                                                                   total_workers,
                                                                   approver_details,
                                                                   report_date_str)

    # And finally, sending the email
    attachment_zip = zip(data_filenames, directorate_files)
    result = send_email(exchange_account, email_message_dict, attachment_zip, message_id, dry_run)

    assert result, f"Email {message_id} did not send successfully"

    logging.info("Generat[ed] directorate level emails")
