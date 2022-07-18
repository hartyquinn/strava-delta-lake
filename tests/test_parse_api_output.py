import pytest
from utils.pipeline_funcs import parse_api_output


def test_parse_api_output_true_if_return_list():
    # Arrange
    json_to_parse = {"resource_state" : 2,
                        "athlete" : {
                            "id" : 134815,
                            "resource_state" : 1
                        },
                        "name" : "Happy Friday",
                        "distance" : 24931.4,
                        "moving_time" : 4500,
                        "elapsed_time" : 4500,
                        "total_elevation_gain" : 0,
                        "type" : "Ride",
                        "workout_type" : None,
                        "id" : 154504250376823,
                        "external_id" : "garmin_push_12345678987654321",
                        "upload_id" : 987654321234567891234,
                        "start_date" : "2018-05-02T12:15:09Z",
                        "start_date_local" : "2018-05-02T05:15:09Z",
                        "timezone" : "(GMT-08:00) America/Los_Angeles",
                        "utc_offset" : -25200,
                        "start_latlng" : None,
                        "end_latlng" : None,
                        "location_city" : None,
                        "location_state" : None,
                        "location_country" : "United States",
                        "achievement_count" : 0,
                        "kudos_count" : 3,
                        "comment_count" : 1,
                        "athlete_count" : 1,
                        "photo_count" : 0,
                        "map" : {
                            "id" : "a12345678987654321",
                            "summary_polyline" : None,
                            "resource_state" : 2
                        },
                        "trainer" : True,
                        "commute" : False,
                        "manual" : False,
                        "private" : False,
                        "flagged" : False,
                        "gear_id" : "b12345678987654321",
                        "from_accepted_tag" : False,
                        "average_speed" : 5.54,
                        "max_speed" : 11,
                        "average_cadence" : 67.1,
                        "average_watts" : 175.3,
                        "weighted_average_watts" : 210,
                        "kilojoules" : 788.7,
                        "device_watts" : True,
                        "has_heartrate" : True,
                        "average_heartrate" : 140.3,
                        "max_heartrate" : 178,
                        "max_watts" : 406,
                        "pr_count" : 0,
                        "total_photo_count" : 1,
                        "has_kudoed" : False,
                        "suffer_score" : 82}
    #Act
    parse_api_output = parse_api_output(json_to_parse)
    x = []
    #Assert
    assert x == parse_api_output_type