import os
from clickhouse_driver import Client
from google.cloud import bigquery


def parse_event(event_data):
    """
    Parses a single event from the JSON data and extracts relevant information.

    Args:
        event_data: A dictionary representing a single event.

    Returns:
        A dictionary containing extracted information from the event.
    """
    event_dict = {
        "event_date": event_data["event_date"],
        "event_timestamp": event_data["event_timestamp"],
        "event_name": event_data["event_name"],
        "user_pseudo_id": event_data["user_pseudo_id"],
        "platform": event_data["platform"],
    }

    # Extract event parameters
    event_params = event_data.get("event_params", [])
    for param in event_params:
        key = param["key"]
        value = param["value"].get("string_value")
        if key and value:
            event_dict[key] = value

    # Extract user properties
    user_properties = event_data.get("user_properties", [])
    for prop in user_properties:
        key = prop["key"]
        value = prop["value"].get("string_value")
        if key and value:
            event_dict[key] = value

    # Extract additional details based on analysis needs (device, geo, app, etc.)
    event_dict["user_first_touch_timestamp"] = event_data.get("user_first_touch_timestamp")
    event_dict["device_category"] = event_data.get("device", {}).get("category")
    event_dict["device_language"] = event_data.get("device", {}).get("language")
    event_dict["geo_continent"] = event_data.get("geo", {}).get("continent")
    event_dict["geo_country"] = event_data.get("geo", {}).get("country")
    event_dict["app_info_id"] = event_data.get("app_info", {}).get("id")
    event_dict["app_info_version"] = event_data.get("app_info", {}).get("version")

    return event_dict


if __name__ == '__main__':

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'test-task-credential.json'

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Connect to ClickHouse server
    clickhouse_driver = Client(host='localhost', port=8123, user='default', password='admin', database='default')

    # Extract data from BigQuery
    sql_query = """
    SELECT 
        * 
    FROM 
        `firebase-public-project.analytics_153293282.events_*`
    """

    # Execute BigQuery query
    query_job = client.query(sql_query)
    results = query_job.result()

    # Process all events
    all_events = []
    for row in results:
        parsed_event = parse_event(row)
        all_events.append(parsed_event)

    print(all_events)

    # Insert data into the users and events tables
    for event in all_events:
        # Insert data into the users table
        user_columns = ['user_pseudo_id', 'user_first_touch_timestamp', 'device_category', 'device_language',
                        'geo_continent', 'geo_country', 'app_info_id', 'app_info_version']
        user_values = [event.get(col, '') for col in user_columns]
        user_query = f"INSERT INTO users ({', '.join(user_columns)}) VALUES ({', '.join(map(repr, user_values))})"
        clickhouse_driver.execute(user_query)

        # Insert data into the events table
        event_columns = ['event_date', 'event_timestamp', 'event_name', 'user_pseudo_id', 'platform',
                         'firebase_screen_class', 'firebase_event_origin', 'board', 'initial_extra_steps',
                         'plays_quickplay', 'num_levels_available', 'firebase_last_notification', 'firebase_exp_4',
                         'plays_progressive', 'firebase_exp_1', 'ad_frequency']
        event_values = [event.get(col, '') for col in event_columns]
        event_query = f"INSERT INTO events ({', '.join(event_columns)}) VALUES ({', '.join(map(repr, event_values))})"
        clickhouse_driver.execute(event_query)

    print("Data loaded into ClickHouse successfully!")
