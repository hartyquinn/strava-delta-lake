import pprint

import requests
import configparser
from typing import Counter, Dict,List
from requests.exceptions import HTTPError
import logging
import boto3
from botocore.exceptions import ClientError
import time
from datetime import datetime, date
import csv
from dateutil.relativedelta import relativedelta 


def connect_to_strava() -> Dict[str,str]:
    """ Get the Strava API authentication tokens when they expire
        Inputs: strava api refresh token, client id, client secret and the oauth URL
        Returns: dict with 2 string:string items. Authorization:Bearer and the Oauth Token """
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    client_id = parser.get("strava_api_config", "client_id")
    client_secret = parser.get("strava_api_config", "client_secret")
    refresh_token = parser.get("strava_api_config", "refresh_token")
    auth_url = parser.get("strava_api_config","auth_url")

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "f": "json",
    }
    try:
        resp = requests.post(url=auth_url, data=payload, verify=False)
        oauth_token = resp.json()["access_token"]
        header = {"Authorization": "Bearer " + oauth_token}
        return header

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')

    except Exception as err:
        print(f'Other error occurred: {err}')


def make_strava_api_request(
    header: Dict[str, str], before:int, after:int, activity_num: int = 100
) -> Dict[str, str]:
    """Use Strava API to get recent page of new data."""
    header = connect_to_strava()
    param = {"per_page": activity_num, "page": 1, 'before':before  , 'after': after}
    
    api_response = requests.get(
            "https://www.strava.com/api/v3/athlete/activities", headers=header, params=param
        ).json()
    return api_response


def parse_api_output(response_json: dict) -> list:
    """Parse output from Strava API."""
    activity = []
    cols_to_extract = [
        "id",
        "name",
        "start_date",
        "start_date_local",
        "timezone",
        "distance",
        "moving_time",
        "elapsed_time",
        "total_elevation_gain",
        "type",
        "workout_type",
        "location_country",
        "achievement_count",
        "kudos_count",
        "comment_count",
        "athlete_count",
        "average_speed",
        "max_speed",
        "average_cadence",
        "average_temp",
        "average_heartrate",
        "max_heartrate",
        "suffer_score",
    ]
    for col in cols_to_extract:
        try:
            activity.append(response_json[col])
        # if col is not found in API repsonse
        except KeyError:
            activity.append(None)
    try:
        start_latlng = response_json["start_latlng"]
        if len(start_latlng) == 2:
            lat, lng = start_latlng[0], start_latlng[1]
            activity.append(lat)
            activity.append(lng)
        else:
            activity.append(None)
            activity.append(None)
    except KeyError:
        activity.append(None)
        activity.append(None)
    return activity


def convert_strava_start_date(date: str) -> datetime:
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    converted_date = datetime.strptime(date, date_format)
    return converted_date


def upload_to_s3(file_name, key=None):
    """Upload a file to an S3 bucket

    :param file_name: File like object to upload
    :param bucket: Bucket to upload to
    :param key: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get("aws_boto_credentials", "access_key")
    secret_key = parser.get("aws_boto_credentials", "secret_key")
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")

    # Upload the file
    s3_client = boto3.client('s3',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key)
    try:
        response = s3_client.upload_file(file_name, bucket_name, key)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def extract_strava_activities() -> List[List]:
    """Connect to Strava API and get data up until last_updated_warehouse datetime."""
    today_int = int((datetime.today()).timestamp())
    prev_month_int = ((datetime.today() - relativedelta(months=1)).timestamp())
    header = connect_to_strava()
    all_activities = []
    activity_num = 100
    

    response_json = make_strava_api_request(header=header, activity_num=activity_num, before=today_int, after=prev_month_int)

    print(type(response_json))

    counter = 0
    for i in response_json:
        activity = parse_api_output(response_json[counter])
        all_activities.append(activity)
        counter += 1
    return all_activities

def save_data_to_bytesIO(all_activities: List[List]):
    """Save extracted data to byte string."""
    todays_date = datetime.today().strftime("%Y_%m_%d")
    with open(f"activities_{todays_date}.csv", "w", newline='') as f:
        headers = [
        "id",
        "name",
        "start_date",
        "start_date_local",
        "timezone",
        "distance",
        "moving_time",
        "elapsed_time",
        "total_elevation_gain",
        "type",
        "workout_type",
        "location_country",
        "achievement_count",
        "kudos_count",
        "comment_count",
        "athlete_count",
        "average_speed",
        "max_speed",
        "average_cadence",
        "average_temp",
        "average_heartrate",
        "max_heartrate",
        "suffer_score",
        "lat",
        "lng",]
        wr = csv.writer(f)
        wr.writerow(headers)
        wr.writerows(all_activities)