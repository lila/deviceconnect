
"""routes for Dexcom data ingestion into bigquery

Configuration:

    * `GOOGLE_CLOUD_PROJECT`: gcp project where bigquery is available.
    * `GOOGLE_APPLICATION_CREDENTIALS`: points to a service account json.
    * `BIGQUERY_DATASET`: dataset to use to store user data.

Notes:

    all the data is ingested into BigQuery tables.
"""

import os
import timeit
from datetime import date, datetime, timedelta
import logging

import pandas as pd
import pandas_gbq
from flask import Blueprint, request
from flask_dance.contrib.dexcom import dexcom as dexcom_session
from authlib.integrations.flask_client import OAuth
from skimpy import clean_columns

from .dexcom_auth import dexcom_bp


log = logging.getLogger(__name__)


bp = Blueprint("dexcom_ingest_bp", __name__)

bigquery_datasetname = os.environ.get("BIGQUERY_DATASET")
if not bigquery_datasetname:
    bigquery_datasetname = "dexcom"


def _tablename(table: str) -> str:
    return bigquery_datasetname + "." + table


@bp.route("/dexcom-ingest")
def dexcom_ingest():
    """test route to ensure that blueprint is loaded"""

    result = []
    allusers = dexcom_bp.storage.all_users()
    log.debug(allusers)

    for x in allusers:

        try:

            log.debug("user = " + x)

            dexcom_bp.storage.user = x
            if dexcom_bp.session.token:
                del dexcom_bp.session.token

            token = dexcom_bp.token

            log.debug("access token: " + token["access_token"])
            log.debug("refresh_token: " + token["refresh_token"])
            log.debug("expiration time " + str(token["expires_at"]))
            log.debug("             in " + str(token["expires_in"]))

            resp = dexcom_session.get("/v2/users/self/dataRange")

            log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

            j = resp.json()

            log.debug(f"retrieved result: {resp.content}")
            result.append(resp.content)

        except (Exception) as e:
            log.error("exception occured: %s", str(e))

    return str(result)


def _normalize_response(df, column_list, email, date_pulled):
    # Fill in missing columns with None
    for col in column_list:
        if col not in df.columns:
            df[col] = None

    # Reorder columns and add id and date
    df = df.reindex(columns=column_list)
    df.insert(0, "id", email)
    df.insert(1, "date", date_pulled)

    # Clean up column names
    df = clean_columns(df)

    return df


def _date_pulled():
    """set the date pulled"""

    date_pulled = date.today() - timedelta(days=1)
    return date_pulled.strftime("%Y-%m-%d")


def _date_today():
    """set the date pulled"""

    date_pulled = date.today()
    return date_pulled.strftime("%Y-%m-%d")

#
# device data
#


@bp.route("/dexcom-devices")
def dexcom_devices():

    start = timeit.default_timer()
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    # if caller provided date as query params, use that otherwise use yesterday
    date_pulled = request.args.get("date", _date_pulled())
    user_list = dexcom_bp.storage.all_users()
    if request.args.get("user") in user_list:
        user_list = [request.args.get("user")]

    log.debug("dexcom-devices:")

    pd.set_option("display.max_columns", 500)

    device_list = []

    for user in user_list:

        log.debug("user: %s", user)

        dexcom_bp.storage.user = user

        if dexcom_bp.session.token:
            del dexcom_bp.session.token

        try:

            params = {
                'startDate': date_pulled,
                'endDate': _date_today()
            }

            resp = dexcom_session.get("/v2/users/self/devices", params=params)

            log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

            devices = resp.json()["devices"]

            if not devices:
                log.debug("no devices found")
                continue

            devices_df = pd.json_normalize(devices)
            devices_columns = [
                "transmitterGeneration",
                "displayDevice",
                "lastUploadDate"
            ]
            devices_df = _normalize_response(
                devices_df, devices_columns, user, date_pulled
            )
            device_list.append(devices_df)

        except (Exception) as e:
            log.error("exception occured: %s", str(e))

    # end loop over users

    #### CONCAT DATAFRAMES INTO BULK DF ####

    load_stop = timeit.default_timer()
    time_to_load = load_stop - start
    print("Program Executed in " + str(time_to_load))

    # ######## LOAD DATA INTO BIGQUERY #########

    log.debug("push to BQ")

    # sql = """
    # SELECT country_name, alpha_2_code
    # FROM `bigquery-public-data.utility_us.country_code_iso`
    # WHERE alpha_2_code LIKE 'A%'
    # """
    # df = pandas_gbq.read_gbq(sql, project_id=project_id)

    if len(device_list) > 0:

        try:

            bulk_device_df = pd.concat(device_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_device_df,
                destination_table=_tablename("dexcom-devices"),
                project_id=project_id,
                if_exists="append",
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {"name": "transmitter_generation", "type": "STRING"},
                    {"name": "display_device", "type": "STRING"},
                    {"name": "last_upload_date", "type": "DATETIME"}
                ],
            )

        except (Exception) as e:
            log.error("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    print("Fitbit Chunk Loaded " + str(execution_time))

    dexcom_bp.storage.user = None

    return "Dexcom devices Loaded"


@bp.route("/dexcom-egvs")
def dexcom_egvs():

    start = timeit.default_timer()
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    # if caller provided date as query params, use that otherwise use yesterday
    date_pulled = request.args.get("date", _date_pulled())
    user_list = dexcom_bp.storage.all_users()
    if request.args.get("user") in user_list:
        user_list = [request.args.get("user")]

    log.debug("dexcom-egvs:")

    pd.set_option("display.max_columns", 500)

    egvs_list = []

    for user in user_list:

        log.debug("user: %s", user)

        dexcom_bp.storage.user = user

        if dexcom_bp.session.token:
            del dexcom_bp.session.token

        try:

            params = {
                'startDate': date_pulled,
                'endDate': _date_today()
            }

            resp = dexcom_session.get("/v2/users/self/egvs", params=params)

            log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

            egvs = resp.json()["egvs"]

            if not evgs:
                continue

            egvs_df = pd.json_normalize(egvs)
            egvs_columns = [
                "systemTime",
                "displayTime",
                "value",
                "realtimeValue",
                "smoothedValue",
                "status",
                "trend",
                "trendRate"
            ]
            egvs_df = _normalize_response(
                egvs_df, egvs_columns, user, date_pulled
            )
            egvs_list.append(egvs_df)

        except (Exception) as e:
            log.error("exception occured: %s", str(e))

    # end loop over users

    #### CONCAT DATAFRAMES INTO BULK DF ####

    load_stop = timeit.default_timer()
    time_to_load = load_stop - start
    print("Program Executed in " + str(time_to_load))

    # ######## LOAD DATA INTO BIGQUERY #########

    log.debug("push to BQ")

    if len(egvs_list) > 0:

        try:

            bulk_egvs_df = pd.concat(egvs_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_egvs_df,
                destination_table=_tablename("dexcom-egvs"),
                project_id=project_id,
                if_exists="append",
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {"name": "system_time", "type": "DATETIME"},
                    {"name": "display_time", "type": "DATETIME"},
                    {"name": "value", "type": "FLOAT"},
                    {"name": "realtime_value", "type": "FLOAT"},
                    {"name": "smoothed_value", "type": "FLOAT"},
                    {"name": "status", "type": "STRING"},
                    {"name": "trend", "type": "STRING"},
                    {"name": "trend_rate", "type": "FLOAT"}
                ],
            )

        except (Exception) as e:
            log.error("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    print("Fitbit Chunk Loaded " + str(execution_time))

    dexcom_bp.storage.user = None

    return "Dexcom egvs Loaded"


@bp.route("/dexcom-events")
def dexcom_events():

    start = timeit.default_timer()
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    # if caller provided date as query params, use that otherwise use yesterday
    date_pulled = request.args.get("date", _date_pulled())
    user_list = dexcom_bp.storage.all_users()
    if request.args.get("user") in user_list:
        user_list = [request.args.get("user")]

    log.debug("dexcom-events:")

    pd.set_option("display.max_columns", 500)

    events_list = []

    for user in user_list:

        log.debug("user: %s", user)

        dexcom_bp.storage.user = user

        if dexcom_bp.session.token:
            del dexcom_bp.session.token

        try:

            params = {
                'startDate': date_pulled,
                'endDate': _date_today()
            }

            resp = dexcom_session.get("/v2/users/self/events", params=params)

            log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

            events = resp.json()["events"]

            if not events:
                continue

            events_df = pd.json_normalize(events)
            events_columns = [
                "systemTime",
                "displayTime",
                "eventType",
                "eventSubType",
                "value",
                "unit"
            ]
            events_df = _normalize_response(
                events_df, events_columns, user, date_pulled
            )
            events_list.append(events_df)

        except (Exception) as e:
            log.error("exception occured: %s", str(e))

    # end loop over users

    #### CONCAT DATAFRAMES INTO BULK DF ####

    load_stop = timeit.default_timer()
    time_to_load = load_stop - start
    print("Program Executed in " + str(time_to_load))

    # ######## LOAD DATA INTO BIGQUERY #########

    log.debug("push to BQ")

    if len(events_list) > 0:

        try:

            bulk_events_df = pd.concat(events_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_events_df,
                destination_table=_tablename("dexcom-events"),
                project_id=project_id,
                if_exists="append",
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {"name": "system_time", "type": "DATETIME"},
                    {"name": "display_time", "type": "DATETIME"},
                    {"name": "event_type", "type": "STRING"},
                    {"name": "event_sub_type", "type": "STRING"},
                    {"name": "value", "type": "FLOAT"},
                    {"name": "unit", "type": "STRING"}
                ],
            )

        except (Exception) as e:
            log.error("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    print("Fitbit Chunk Loaded " + str(execution_time))

    dexcom_bp.storage.user = None

    return "Dexcom events Loaded"
