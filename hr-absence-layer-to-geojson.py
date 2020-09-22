# base imports
from datetime import timedelta
import json
import logging
import os
import pathlib
import sys
import tempfile
# external imports
from db_utils import minio_utils
import geopandas as gpd
from h3 import h3
import pandas as pd
from shapely import wkt
from shapely.geometry import Point
from shapely.geometry import Polygon
# local imports
from hr_data_last_values_to_minio import get_current_hr_df, merge_df, directorate_filter_df
# import minio args
from hr_data_last_values_to_minio import MINIO_BUCKET, MINIO_CLASSIFICATION, DATA_RESTRICTED_PREFIX 
# import variable settings
from hr_data_last_values_to_minio import STATUS_WINDOW_LENGTH, WORKING_STATUS, NOT_WORKING_STATUS, STATUSES_TO_SUCCINCT_MAP 
from hr_data_last_values_to_minio import STAFF_NUMBER_COL_NAME, DATE_COL_NAME, STATUS_COL, SUCCINCT_STATUS_COL
# import data file variable name
from hr_data_last_values_to_minio import HR_DATA_FILENAME


__author__ = "Colin Anthony"


# annotation datasets
DATA_PUBLIC_PREFIX = "data/public/"
CITY_BUILDINGS = "city-buildings-locations.csv"
HR_ANNOT = "city_people.csv"
CITY_PEOPLE_LOCS = "city-people-locations-updated-2020-06-19.csv" 

HEX_LEVEL = 6
HEX_ABSENCE_INDEX_PROPERTY = "index"
ADDRESS_COL = "FebMostCommonClockingLocation"
DIR_COL = "Directorate"
DEPT_COL = "Department"
POS_COL = "Position"

POINT_OUTPUT_FILENAME_SUFFIX = "absence-counts-points.geojson"
HEX_OUTPUT_FILENAME_SUFFIX = "absence-counts-hex.geojson"

SECRETS_PATH_VAR = "SECRETS_PATH"


def get_minio_to_df(minio_filename_override, minio_key, minio_secret):
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                           minio_filename_override=minio_filename_override,
                                           minio_bucket=MINIO_BUCKET,
                                           minio_key=minio_key,
                                           minio_secret=minio_secret,
                                           data_classification=MINIO_CLASSIFICATION,
                                           )
        if not result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...") 
            df = pd.read_csv(temp_data_file)
            return df


def position_harmoniser(x):
    """
    Function to condence multiple versions of a City postion down to a signle aggregagted entry
    eg: Senior Clerk, Junior Clerk, Specialist Clerk == Clerk (All)
    Args: 
        x (str): position entry
    Returns:
        x (str): modified position entry if entry is in items list
    """
    # if isna, return nan
    if pd.isna(x):
        return x
    items = ["Clerk", "Nurse", "Driver", "Foreman", "Worker", "Handyman", "Legal Advisor", "Controller", "Field Ranger",  
             "Operator", "Engineering", "Construction", "Fire Fighter", "Law Enforcement Officer", "Courier", "Workman",
            "Coordinator", "Manager", "Meter Reader", "Technician", "Payroll", "Head", "Technical Assistant", "Superintendent",
            "Professional Officer", "Mainlayer", "Dispatcher", "Administrator", "Water Pollution Control Inspector", "Water Pollution Control Officer", 
             "Sampler", "Laboratory Assistant", "Secretary", "Caulker", "Auditor", "Tea Server", "SAP Developer", "GIS Analyst", "IT Systems Engineer",
            "Software Deployer", "SAP Developer", "Valuer", "Spatial Data Integration Specialist", "Security Officer"]
    
    for i in items:
        # return condensed title
        if i in x:
            x = i + " (all)"
            return x
   
    # else return the original title
    return x


def get_hex(point, resolution=HEX_LEVEL):
    """
    Convert Shapely Point coordinates into H3 Uber Hex index codes
    Args:
        point (obj): shapely Point object
        resolution (int): the Uber Hex resolution/size - an int from 0-15 .
    Returns:
        hex_code (str): the Uber hex code relating to the lat-lng coordinates
    """
    lat = point.y
    lng = point.x
    hex_code = h3.geo_to_h3(lat, lng, resolution)
    return hex_code


def fix_hr_data_names(hr_data_df, position_header="Position", dept_header="Department", directorate_header="Directorate"):
    """
    Function to sanitise hr form position names, and directorate and department fields
    Args:
        hr_data_df (obj): pandas dataframe with CoCT columns for position, department and directorate
        position_header (str, optional): the column name containig the Positions. Defaults to "Position".
        dept_header (str, optional): the column name containig the Departments. Defaults to "Department".
        directorate_header (str, optional): the column name containig the Directorates. Defaults to "Directorate".

    Returns:
        (obj): input pandas dataframe with modified values
    """
    
    df_headers = list(hr_data_df)
    # format the position names
    if position_header:
        # make sure the header is in the df
        if position_header not in df_headers:
            logging.error(f"{position_header} not in dataframe headers: {df_headers}")
            sys.exit(-1)
            
        POSITION_REMAP = (
            ("worker", "Worker", False, False),
            ("Center", "Centre", False, False),
            ("clerk", "Clerk", False, False),
            ("Asst", "Assistant", False, False),
            ("Speciali", "Specialist", False, False),
            ("Special", "Specialist", False, False),
            ("Managem", "Management", False, False),
            ("Senioressional", "Senior essentional", False, False),
            ("Co-ordinator", "Coordinator", False, False),
            ("Assist.*", "Assistant", True, False),
            ("Agent.*", "Agent", True, False),
            ("Officer.*", "Officer", True, False),
            ("Admin.*", "Administrator", True, False),
            ("Operational Supervisor-.*", "Operational Supervisor", True, False),
            (",", "", False, False),
            (" - ", "-", False, False),
            ("/", "-", False, False),
            ("and ", "& ", False, False),
            
        )

        for old_pos_str, sub_pos_str, regex_status, case_status in POSITION_REMAP:
            hr_data_df.loc[:, position_header] = hr_data_df[position_header].str.replace(
                old_pos_str, sub_pos_str, regex=regex_status, case=case_status)

        hr_data_df.loc[:, position_header] = hr_data_df[position_header].str.strip(")(:,")

        # harmonise naming to one type: senior, junior, specialist = (all)
        hr_data_df.loc[:, position_header] = hr_data_df[position_header].apply(position_harmoniser) 
    
    # format dept names
    if dept_header:
        DEPT_REMAP = (
            ("and ", "& ", False, False),
        )
        # make sure the header is in the df
        if dept_header not in df_headers:
            logging.error(f"{dept_header} not in dataframe headers: {df_headers}")
            sys.exit(-1)
            
        for old_pos_str, sub_pos_str, regex_status, case_status in DEPT_REMAP:
            hr_data_df.loc[:, dept_header] = hr_data_df[dept_header].str.replace(
                old_pos_str, sub_pos_str, regex=regex_status, case=case_status)

    # format Dir names
    if directorate_header:
        DIR_REMAP = (
            ("&", "& ", False, False),
            ("and ", "& ", False, False),
            ("  ", " ", False, False)
        )
        # make sure the header is in the df
        if directorate_header not in df_headers:
            logging.error(f"{directorate_header} not in dataframe headers: {df_headers}")
            sys.exit(-1)
        for old_pos_str, sub_pos_str, regex_status, case_status in DIR_REMAP:
            hr_data_df.loc[:, directorate_header] = hr_data_df[directorate_header].str.replace(
                old_pos_str, sub_pos_str, regex=regex_status, case=case_status)

    return hr_data_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')
    
    # Loading secrets
    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        sys.exit(-1)

    logging.info("Setting secrets variables")
    secrets_path = os.environ["SECRETS_PATH"]

    if not os.path.exists(secrets_path):
        logging.error(f"Secrets path {secrets_path} does not exist, check the ENV variable: {SECRETS_PATH_VAR}")
        sys.exit(-1)
    
    secrets = json.load(open(secrets_path))
    
    directorate_file_prefix = sys.argv[1]
    directorate_title = sys.argv[2]
    logging.debug(f"directorate_file_prefix={directorate_file_prefix}, directorate_title={directorate_title}")
    
    # _________________________________________________________________
    # get the data
    logging.info(f"Fetch[ing] {CITY_BUILDINGS} data from Minio bucket...")
    city_building_locs = get_minio_to_df(
        minio_filename_override=f"{DATA_PUBLIC_PREFIX}{CITY_BUILDINGS}",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    logging.info(f"Fetch[ed] {CITY_BUILDINGS}")
    city_building_locs = city_building_locs[["Name", "EWKT", "MportalLayer", "OBJECTID"]].copy()
    city_building_locs.rename(columns={"Name": "NearestCityFacility", "EWKT": "BLD_EWKT", "OBJECTID": "NearestCityFacilityId"}, inplace=True)

    logging.info(f"Fetch[ing] {HR_DATA_FILENAME} data from Minio bucket...")
    hr_transactional_data_df = get_minio_to_df(
            minio_filename_override=f"{DATA_RESTRICTED_PREFIX}{HR_DATA_FILENAME}",
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
    logging.info(f"Fetch[ed] {HR_DATA_FILENAME}")
    logging.info(f"Fetch[ing] {HR_ANNOT} data from Minio bucket...")
    hr_master_data_df = get_minio_to_df(
            minio_filename_override=f"{DATA_RESTRICTED_PREFIX}{HR_ANNOT}",
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
    logging.info(f"Fetch[ed] {HR_ANNOT}")

    # _________________________________________________________________
    # merge the annotations to the dataset
    logging.info("Merg[ing] data...")
    hr_combined_df = merge_df(hr_transactional_data_df, hr_master_data_df)
    logging.info("...Merg[ed] data")

    # _________________________________________________________________
    # filter data to directorate
    logging.info("Filter[ing] data...")
    hr_data_directorate = directorate_filter_df(hr_combined_df, directorate_title)
    logging.info("Filter[ed] data...")

    if hr_data_directorate.empty:
        logging.error(f"Empty Dataframe for {directorate_file_prefix} filter")
        sys.exit(-1)

    # _________________________________________________________________
    # filter the dataframes to only required cols
    hr_data_directorate = hr_data_directorate[
        [DATE_COL_NAME, STAFF_NUMBER_COL_NAME, STATUS_COL, "ApproverStaffNumber",
         DIR_COL, DEPT_COL, POS_COL, "Org Unit Name",
         ADDRESS_COL, "EWKT", "NearestCityFacility", "MportalLayer",
         "NearestCityFacilityId"]
    ].copy()

    # _________________________________________________________________
    # fix typos
    logging.info(f"Cleaning up position labels in annotation df")
    hr_data_directorate_filt = fix_hr_data_names(hr_data_directorate, position_header=POS_COL, dept_header=DEPT_COL, directorate_header=DIR_COL)

    # _________________________________________________________________
    # get the totals for each position by department
    logging.info(f"Calculating the position totals per department")
    job_count_totals = hr_data_directorate_filt.groupby([DIR_COL, DEPT_COL, POS_COL])[POS_COL].size().rename('position_total').reset_index()

    # _________________________________________________________________
    # merge the annotations to the dataset
    logging.info(f"Merging annotations onto business continuity data")
    logging.debug(f"Annotations to hr data merge: pre-merge_shape = {hr_data_directorate_filt.shape}")
    hr_data_filt_annot = pd.merge(hr_data_directorate_filt, city_building_locs, on=["NearestCityFacility", "MportalLayer", "NearestCityFacilityId"], how="left", validate="many_to_one")
    logging.debug(f"Annotations to hr data merge: post-merge_shape = {hr_data_filt_annot.shape}")

    # set date object
    logging.info(f"Format the Date column")
    hr_data_filt_annot.loc[:, DATE_COL_NAME] = pd.to_datetime(hr_data_filt_annot[DATE_COL_NAME])

    logging.info(f"Categorise working statuses")
    hr_data_filt_annot.loc[:, SUCCINCT_STATUS_COL] = hr_data_filt_annot[STATUS_COL].map(STATUSES_TO_SUCCINCT_MAP)

    # convert working status to binary for counts
    MAP_STATUS_FOR_COUNTS = {
        WORKING_STATUS: 0,
        NOT_WORKING_STATUS: 1,
    }
    hr_data_filt_annot.loc[:, "not_work_status"] = hr_data_filt_annot[SUCCINCT_STATUS_COL].map(MAP_STATUS_FOR_COUNTS)

    # _________________________________________________________________
    # filter to only most recent assessemnt for each staff memeber
    logging.info(f"Filtering to most recent entry for each staff member")
    most_recent_ts, hr_data_latest = get_current_hr_df(hr_data_filt_annot)

    logging.info(f"Splitting by entries with valid location points")
    # split out entries with no geolocations from those with coordinates
    hr_data_latest_no_loc = hr_data_latest[hr_data_latest['EWKT'].isnull()].copy()
    hr_data_latest_with_loc = hr_data_latest[hr_data_latest['EWKT'].notnull()].copy()

    logging.debug(f"Staff with location data in last cycle = {len(hr_data_latest_with_loc)}")
    logging.debug(f"Staff with no location data in last cycle = {len(hr_data_latest_no_loc)}")

    # _________________________________________________________________
    # make df with building WKT
    # make wkt from ewkt
    hr_data_latest_building_loc = hr_data_latest_with_loc.copy()
    hr_data_latest_building_loc.loc[:, "srid"] = hr_data_latest_building_loc["BLD_EWKT"].apply(
        lambda x: x.split(";")[0].replace("SRID=", "") if not pd.isna(x) else x)
    hr_data_latest_building_loc.loc[:, "WKT"] = hr_data_latest_building_loc["BLD_EWKT"].apply(
        lambda x: x.split(";")[1] if not pd.isna(x) else x)
    # make geodataframe
    crs = hr_data_latest_building_loc["srid"].unique()
    if len(crs) > 1:
        logging.error("Danger, different projections in the dataset")
        sys.exit(-1)

    hr_data_latest_building_loc.loc[:, 'WKT'] = hr_data_latest_building_loc['WKT'].apply(wkt.loads)
    hr_data_latest_building_loc = gpd.GeoDataFrame(hr_data_latest_building_loc, geometry='WKT', crs=f'epsg:{crs[0]}')
    hr_data_latest_building_loc.drop(["EWKT", "srid"], axis="columns", inplace=True)
    # add uberhex index
    hr_data_latest_building_loc.loc[:, HEX_ABSENCE_INDEX_PROPERTY] = hr_data_latest_building_loc["WKT"].apply(get_hex, resolution=HEX_LEVEL)

    # _________________________________________________________________
    # make df with Staff Point WKT
    # make wkt from ewkt
    hr_data_latest_staff_loc = hr_data_latest_with_loc.copy()

    hr_data_latest_staff_loc.loc[:, "srid"] = hr_data_latest_staff_loc["EWKT"].apply(lambda x: x.split(";")[0].replace("SRID=", "") if not pd.isna(x) else x)
    hr_data_latest_staff_loc.loc[:, "WKT"] = hr_data_latest_staff_loc["EWKT"].apply(lambda x: x.split(";")[1] if not pd.isna(x) else x)
    # make geodataframe
    crs = hr_data_latest_staff_loc["srid"].unique()
    if len(crs) > 1:
        logging.error("Danger, differnt projections in the dataset")
        sys.exit(-1)
    hr_data_latest_staff_loc.loc[:, 'WKT'] = hr_data_latest_staff_loc['WKT'].apply(wkt.loads)
    hr_data_latest_staff_loc = gpd.GeoDataFrame(hr_data_latest_staff_loc, geometry='WKT', crs=f'epsg:{crs[0]}')
    hr_data_latest_staff_loc.drop(["EWKT", "srid"], axis="columns", inplace=True)
    # add uberhex index
    hr_data_latest_staff_loc.loc[:, HEX_ABSENCE_INDEX_PROPERTY] = hr_data_latest_staff_loc["WKT"].apply(get_hex, resolution=HEX_LEVEL)

    # _________________________________________________________________
    # get hex counts for building locations

    # get the absence counts and total assessed per index
    latest_work_status_counts_hex = hr_data_latest_building_loc.groupby(["index"]).agg(
        absent_count = ("not_work_status", "sum"),
        total_assessed = ("not_work_status", "size"),
    ).reset_index()

    # calculate the percent absent
    latest_work_status_counts_hex.loc[:, "percent_absent"] = round(latest_work_status_counts_hex["absent_count"] / latest_work_status_counts_hex["total_assessed"] * 100, 1)
    # add the assessment window end date
    latest_work_status_counts_hex = latest_work_status_counts_hex.assign(assessment_end_date = hr_data_latest_building_loc.Date.max().strftime("%Y-%m-%d"))
    # add the polycons by H3 hex id use geo_json=True else long lat are swapped
    latest_work_status_counts_hex.loc[:, "WKT"] = latest_work_status_counts_hex["index"].apply(lambda x: Polygon(h3.h3_to_geo_boundary(x, geo_json=True)))

    # _________________________________________________________________
    # get points count for staff locations

    # deduplicate the staff address to location lookup
    staff_address_wkt_lookup = hr_data_latest_staff_loc.drop_duplicates(subset=ADDRESS_COL, keep="first")

    # get the absence counts and total assessed per address
    latest_work_status_counts_point = hr_data_latest_staff_loc.groupby([ADDRESS_COL]).agg(
        absent_count = ("not_work_status", "sum"),
        total_assessed = ("not_work_status", "size"),
    ).reset_index()

    # calculate the percent absent
    latest_work_status_counts_point.loc[:, "percent_absent"] = round(latest_work_status_counts_point["absent_count"] / latest_work_status_counts_point["total_assessed"] * 100, 1)
    # add the assessment window end date
    latest_work_status_counts_point = latest_work_status_counts_point.assign(assessment_end_date = hr_data_latest_staff_loc.Date.max().strftime("%Y-%m-%d"))
    # add the points back in by mapping to the address
    latest_work_status_counts_point = pd.merge(
        latest_work_status_counts_point, staff_address_wkt_lookup[["FebMostCommonClockingLocation", "WKT"]], on="FebMostCommonClockingLocation", how="left", validate="many_to_one")

    # _________________________________________________________________
    # convert to geopandas
    latest_work_status_counts_hex = gpd.GeoDataFrame(latest_work_status_counts_hex, geometry='WKT', crs=f'epsg:{crs[0]}')
    latest_work_status_counts_point = gpd.GeoDataFrame(latest_work_status_counts_point, geometry='WKT', crs=f'epsg:{crs[0]}')

    # _________________________________________________________________
    # write geojson to minio
    logging.info(f"Writing geojson files to minio")
    temp_path = pathlib.Path(tempfile.TemporaryDirectory().name)
    temp_path.mkdir()
    directorate_file_prefix = directorate_file_prefix.replace("_", "-")
    out_points_geojson = pathlib.Path(temp_path, f"{directorate_file_prefix}-{POINT_OUTPUT_FILENAME_SUFFIX}")
    out_hex_geojson =  pathlib.Path(temp_path, f"{directorate_file_prefix}-{HEX_OUTPUT_FILENAME_SUFFIX}")

    if out_points_geojson.exists():
        os.unlink(out_points_geojson)
    if out_hex_geojson.exists():
        os.unlink(out_hex_geojson)

    latest_work_status_counts_point.to_file(out_points_geojson, driver="GeoJSON")
    latest_work_status_counts_hex.to_file(out_hex_geojson, driver="GeoJSON")

    file_tuples = [(out_hex_geojson, latest_work_status_counts_hex), (out_points_geojson, latest_work_status_counts_point)]
    for (file_name, df) in file_tuples:
        minio_utils.file_to_minio(
            filename=file_name,
            minio_bucket=MINIO_BUCKET,
            filename_prefix_override=DATA_RESTRICTED_PREFIX,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=MINIO_CLASSIFICATION,
    )

    logging.debug(f"Done")
