#!/usr/bin/env python3

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
import jinja2
import pandas

BUCKET = "covid"
CLASSIFICATION = minio_utils.DataClassification.EDGE
HR_TRANSACTIONAL_FILENAME_PATH = "data/private/business_continuity_people_status.csv"
HR_MASTER_FILENAME_PATH = "data/private/city_people.csv"
HR_ORG_UNIT_STATUSES = "data/private/business_continuity_org_unit_statuses.csv"

HR_STAFFNUMBER = "StaffNumber"
DATE_COL = "Date"
DIRECTORATE_COL = "Directorate"
CATEGORY_COL = "Categories"
EVALUATION_COL = "Evaluation"
ORG_HIERACHY = ["Directorate", "Department", "Branch", "Section", "Division", "Div Sub Area", "Unit", "Subunit"]
ESSENTIAL_COL = "EssentialStaff"
APPROVER_COL = "Approver"
HR_PEOPLE_SHARE_COLS = [HR_STAFFNUMBER, DATE_COL, "Position", "First name", "Last name", "Org Unit Name",
                        APPROVER_COL, "ApproverStaffNumber",
                        CATEGORY_COL, EVALUATION_COL,
                        *ORG_HIERACHY,
                        "FebMostCommonClockingLocation", ]
APPROVER_MASTER_COL = "Approver Name"
HR_MISSING_PEOPLE_SHARE_COLS = [HR_STAFFNUMBER, "Position", "First name", "Last name", "Org Unit Name",
                                APPROVER_MASTER_COL, "Approver Staff No",
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
    #      "cc_email": DATASCIENCE_CREW + HR_CREW},
    # "COMMUNITY SERVICES and HEALTH":
    #     {"receiver_name": ["Hennie"],
    #      "receiver_email": ["Hendrik.Viviers@capetown.gov.za"],
    #      "cc_email": ["Lele.Sithole@capetown.gov.za"]},
    # "SAFETY AND SECURITY":
    #     {"receiver_name": ["Althea"],
    #      "receiver_email": ["Althea.Daniels@capetown.gov.za"],
    #      "cc_email": ["Lele.Sithole@capetown.gov.za"]},
    # "ENERGY AND CLIMATE CHANGE":
    #     {"receiver_name": ["Maurietta"],
    #      "receiver_email": ["Maurietta.Page@capetown.gov.za"],
    #      "cc_email": ["Lele.Sithole@capetown.gov.za"]},
    # "FINANCE":
    #     {"receiver_name": ["Tembekile"],
    #      "receiver_email": ["Tembekile.Solanga@capetown.gov.za"],
    #      "cc_email": ["Lele.Sithole@capetown.gov.za"]},
    "CORPORATE SERVICES":
        {"receiver_name": ["Phindile"],
         "receiver_email": ["PreciousPhindile.Dlamini@capetown.gov.za"],
         "cc_email": DATASCIENCE_CREW + HR_CREW},
    "TRANSPORT":
        {"receiver_name": ["Louise"],
         "receiver_email": ["Louise.Burger@capetown.gov.za"],
         "cc_email": DATASCIENCE_CREW + HR_CREW},
    "ECONOMIC OPPORTUNITIES &ASSET MANAGEMENT":
        {"receiver_name": ["Roline"],
         "receiver_email": ["Roline.Henning@capetown.gov.za"],
         "cc_email": DATASCIENCE_CREW + HR_CREW},
    "SPATIAL PLANNING AND ENVIRONMENT":
        {"receiver_name": ["Leonie"],
         "receiver_email": ["Leonie.Kroese@capetown.gov.za"],
         "cc_email": DATASCIENCE_CREW + HR_CREW},
    "HUMAN SETTLEMENTS":
        {"receiver_name": ["Gerard"],
         "receiver_email": ["Gerard.Joyce@capetown.gov.za"],
         "cc_email": DATASCIENCE_CREW + HR_CREW},
    "URBAN MANAGEMENT":
        {"receiver_name": ["Sibusiso"],
         "receiver_email": ["Sibusiso.Mayekiso@capetown.gov.za"],
         "cc_email": DATASCIENCE_CREW + HR_CREW},
    "CITY MANAGER":
        {"receiver_name": ["Phindile"],
         "receiver_email": ["PreciousPhindile.Dlamini@capetown.gov.za"],
         "cc_email": DATASCIENCE_CREW + HR_CREW},
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


def get_today_directorate_df(data_df, directorate, date_filter=True):
    logging.debug(f"data_df.head(5)=\n{data_df.head(5)}")
    today = datetime.datetime.now().strftime(ISO8601_DATE_FORMAT)
    logging.debug(f"directorate={directorate}, today={today}")
    query_df = data_df.loc[
        (data_df[DIRECTORATE_COL] == directorate) &
        ((data_df[DATE_COL] == today) if date_filter else True)
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


def get_email_stats(directorate_merged_df, directorate_master_df):
    assessed_ess_workers = directorate_merged_df[ESSENTIAL_COL].sum()
    total_ess_workers = directorate_master_df[ESSENTIAL_COL].sum()

    return assessed_ess_workers, total_ess_workers


def get_missing_workers_df(directorate_merged_df, directorate_master_df):
    missing_ess_df = directorate_master_df.loc[
        directorate_master_df[ESSENTIAL_COL] &
        ~directorate_master_df[HR_STAFFNUMBER].isin(directorate_merged_df[HR_STAFFNUMBER])
    ]
    logging.debug(f"missing_ess_df.head(5)=\n{missing_ess_df.head(5)}")
    logging.debug(f"missing_ess_df.columns=\n{missing_ess_df.columns}")

    missing_ess_df.sort_values([APPROVER_MASTER_COL, *ORG_HIERACHY], inplace=True)

    return missing_ess_df


def write_directorate_file(*dfs):
    for data_df in dfs:
        with tempfile.NamedTemporaryFile("rb", suffix=".xlsx") as df_tempfile:
            data_df.fillna("").to_excel(df_tempfile.name, index=False)

            yield df_tempfile


def load_email_template(email_filename):
    # Loading email template
    with open(email_filename, "r") as email_file:
        email_template = jinja2.Template(email_file.read())

    return email_template


def render_email(email_template, receiver_dict, directorate,
                 assessed_ess_workers, total_ess_workers, missing_approvers):
    receiver_name = receiver_dict["receiver_name"]
    receiver_name_string = receiver_name[0]

    # Handling any middle receivers
    if len(receiver_name) > 2:
        receiver_name_string = receiver_name_string + ", " + ", ".join(receiver_name[1:-1])

    # Handling the salutation end
    if len(receiver_name) > 1:
        receiver_name_string = receiver_name_string + f" and {receiver_name[-1]}"

    now = datetime.datetime.now()
    iso8601_date = now.strftime(ISO8601_DATE_FORMAT)
    iso8601_timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f%Z")

    directorate_filename = directorate.lower().replace(" ", "_")
    directorate_employee_status_filename = f"{directorate_filename}_employee_status_{iso8601_date}.xls"
    directorate_missing_employee_filename = f"{directorate_filename}_employee_not_approved_{iso8601_date}.xls"
    directorate_org_unit_status_filename = f"{directorate_filename}_org_unit_status_{iso8601_date}.xls"

    message_id = str(uuid.uuid4())

    body_dict = dict(
        directorate=directorate,
        receiver_name=receiver_name_string,
        iso8601_date=iso8601_date,
        iso8601_timestamp=iso8601_timestamp,
        directorate_employee_status_filename=directorate_employee_status_filename,
        directorate_org_unit_status_filename=directorate_org_unit_status_filename,
        directorate_missing_employee_filename=directorate_missing_employee_filename,
        request_id=message_id,
        assessed_ess_workers=assessed_ess_workers,
        total_ess_workers=total_ess_workers,
        missing_names=missing_approvers,
    )
    logging.debug(f"template_dict=\n{pprint.pformat(body_dict)}")
    body = email_template.render(**body_dict)

    subject = EMAIL_SUBJECT_TEMPLATE.format(directorate, iso8601_date)
    message_dict = dict(subject=subject,
                        body=HTMLBody(body),
                        to_recipients=receiver_dict["receiver_email"],
                        cc_recipients=receiver_dict["cc_email"],
                        reply_to=DATASCIENCE_CREW, )

    return (message_id, message_dict,
            directorate_employee_status_filename,
            directorate_org_unit_status_filename,
            directorate_missing_employee_filename)


def send_email(account, email_message_dict, attachments, message_uuid):
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
        message.attach(
            FileAttachment(name=attachment_name, content=attachment_file.read())
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

    message.send(save_copy=True)

    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

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
    for directorate, directorate_dict in DIRECTORATE_DETAILS_DICT.items():
        # Getting directorate level data
        hr_combined_people_df[DATE_COL] = pandas.to_datetime(
            hr_combined_people_df[DATE_COL], format="%Y-%m-%dT%H:%M:%S"
        ).dt.strftime(ISO8601_DATE_FORMAT)
        directorate_people_df = get_today_directorate_df(hr_combined_people_df, directorate)
        directorate_master_people_df = get_today_directorate_df(hr_master_df, directorate, date_filter=False)

        hr_org_unit_df[DATE_COL] = pandas.to_datetime(
            hr_org_unit_df[DATE_COL], format=ISO8601_DATE_FORMAT
        ).dt.strftime(ISO8601_DATE_FORMAT)
        directorate_org_df = get_today_directorate_df(hr_org_unit_df, directorate)

        if directorate_people_df.shape[0] == 0 or directorate_org_df.shape[0] == 0:
            logging.debug("Skipping because one of the DFs is empty...")
            continue

        # Various DF calculations
        directorate_people_df = directorate_people_df
        directorate_missing_people_df = get_missing_workers_df(directorate_people_df, directorate_master_people_df)
        directorate_org_pivot_df = get_pivot_df(directorate_org_df)

        assessed_essential_workers, total_esseential_workers = get_email_stats(directorate_people_df, directorate_master_people_df)

        directorate_files = write_directorate_file(directorate_people_df[HR_PEOPLE_SHARE_COLS],
                                                   directorate_org_pivot_df,
                                                   directorate_missing_people_df[HR_MISSING_PEOPLE_SHARE_COLS])

        # Rendering email
        approvers_with_missing = directorate_missing_people_df[APPROVER_MASTER_COL].unique()
        message_id, email_message_dict, *data_filenames = render_email(hr_email_template,
                                                                       directorate_dict,
                                                                       directorate,
                                                                       assessed_essential_workers,
                                                                       total_esseential_workers,
                                                                       approvers_with_missing)

        # And finally, sending the email
        attachment_zip = zip(data_filenames, directorate_files)
        result = send_email(exchange_account, email_message_dict, attachment_zip, message_id)

        assert result, f"Email {message_id} did not send successfully"

    logging.info("Generat[ed] directorate level emails")
