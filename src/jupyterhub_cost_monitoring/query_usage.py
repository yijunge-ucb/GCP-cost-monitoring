"""
Query the Prometheus server to get usage of JupyterHub resources.
"""

import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List
import psycopg2


import requests

from .cache import ttl_lru_cache
from .date_utils import DateRange
from .logs import get_logger
from .const_cost_gcp import \
    HUBS, NODEPOOL_TO_HUBS_MAPPING, \
    HUB_TO_NODEPOOL_MAPPLING, \
    HUB_TO_FILESTORE_MAPPLING, FILESTORE_TO_HUBS_MAPPING, \
    PROMETHEUS_HOST, PROMETHEUS_PORT, \
    DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_TABLE_PREFIX

logger = get_logger(__name__)


@ttl_lru_cache(seconds_to_live=3600)
def query_prometheus(query: str, date_range: DateRange, step: str) -> requests.Response:
    """
    Query the Prometheus server with the given query over a date range.

    Args:
        query: The Prometheus query string
        date_range: DateRange object containing the time period for the query
        step: The query resolution step duration

    Returns:
        JSON response from Prometheus API
    """
    start_time = time.time()
    # Use Prometheus-formatted dates (inclusive date range with ISO timestamps)
    from_date, to_date = date_range.prometheus_range
    query_api = f"http://{PROMETHEUS_HOST}:{PROMETHEUS_PORT}/api/v1/query_range"

    params = {
        "query": query,
        "start": from_date,
        "end": to_date,
        "step": step,
    }

    try:
        response = requests.get(query_api, params=params, timeout=600)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Prometheus query failed: {e}\nResponse body: {response.text}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Error querying Prometheus: {e}")
        raise

    result = response.json()
    elapsed = time.time() - start_time
    series_count = len(result.get("data", {}).get("result", []))
    logger.info(
        f"query_prometheus finished in {elapsed:.2f}s, returned {series_count} series"
    )
    return result



@ttl_lru_cache(seconds_to_live=7200)
def memory_usage_all_nodepools(date_range: DateRange):
    """
    Query memory usage for all hubs in all nodepools from Prometheus in a single call.
    Returns a dict keyed by (user, hub, date).
    """
    # Build a regex for all hubs in all nodepools
    all_hubs = set()
    for hubs in NODEPOOL_TO_HUBS_MAPPING.values():
        all_hubs.update(hubs)
    hub_regex = "|".join(sorted(all_hubs))
    if not hub_regex:
        return {}

    query = f"""
        label_replace(
            sum(
                kube_pod_container_resource_requests{{resource="memory", namespace=~"{hub_regex}"}}
                * on (namespace, pod)
                  group_left(annotation_hub_jupyter_org_username)
                  group(
                      kube_pod_annotations{{
                          annotation_hub_jupyter_org_username!="",
                          namespace=~"{hub_regex}"
                      }}
                  ) by (pod, namespace, annotation_hub_jupyter_org_username)
            ) by (annotation_hub_jupyter_org_username, namespace)
        ,
        "username", "$1", "annotation_hub_jupyter_org_username", "(.*)"
        )
    """

    response = query_prometheus(query, date_range, step="5m")
    processed = _process_response(response, component_name="compute")

    output = {}
    for entry in processed:
        user = entry["user"]
        hub = entry["hub"]
        date = entry["date"]
        value = entry["value"]
        output[(user, hub, date)] = output.get((user, hub, date), 0.0) + value

    # Collect hubs and dates that truly appear in data
    hubs_with_data = {e["hub"] for e in processed}
    dates_with_data = {e["date"] for e in processed}

    # Per hub totals
    for hub in hubs_with_data:
        for date in dates_with_data:
            hub_total = sum(
                output.get((user, hub, date), 0.0)
                for (user, h, d) in output.keys()
                if h == hub and d == date and user != "all_users"
            )
            output[("all_users", hub, date)] = hub_total

    # Per nodepool totals
    for nodepool, hubs in NODEPOOL_TO_HUBS_MAPPING.items():
        for date in dates_with_data:
            nodepool_total = sum(
                output.get(("all_users", hub, date), 0.0)
                for hub in hubs
            )
            output[("all_hubs", nodepool, date)] = nodepool_total

    return output


def memory_usage_per_hub(date_range: DateRange, hub_name: str):
    """
    Efficiently get memory usage for a specific hub by batching/caching at the all-nodepools level.
    """
    nodepool_name = HUB_TO_NODEPOOL_MAPPLING.get(hub_name)
    all_usage = memory_usage_all_nodepools(date_range)
    # Only keep entries for this hub or for nodepool totals
    filtered = {(user, hub, date): value for (user, hub, date), value in all_usage.items()
                if hub == hub_name or (user == "all_hubs" and hub == nodepool_name)}
    return filtered


@ttl_lru_cache(seconds_to_live=7200)
def filestore_usage_all_hubs(date_range: DateRange):
    """
    Query filestore usage for all hubs in all filestores in a single Prometheus call.
    Returns a dict keyed by (user, hub, date).
    Directory names are mapped to usernames using the database.
    """
    # Build a regex for all hubs in all filestores
    all_hubs = set()
    for hubs in FILESTORE_TO_HUBS_MAPPING.values():
        all_hubs.update(hubs)
    hub_regex = "|".join(sorted(all_hubs))
    if not hub_regex:
        return {}

    query = f"""
    label_replace(
        dirsize_total_size_bytes{{namespace=~"{hub_regex}"}},
        "username", "$1", "directory", "(.*)"
    )
    """

    response = query_prometheus(query, date_range, step="1d")
    processed = _process_response(response, component_name="filestore")

    logger.info(f"[filestore_usage_all_hubs] Processing {len(processed)} entries from Prometheus.")

    # Build a mapping from hub to dir_name->username
    hub_to_dir_map = {}
    for hub in all_hubs:
        table_name = DB_TABLE_PREFIX + hub.replace("-", "_")
        try:
            hub_to_dir_map[hub] = get_dir_to_username_map(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, table_name)
        except Exception as e:
            logger.error(f"[filestore_usage_all_hubs] Error processing dir_name->username mapping for hub '{hub}': {e}")
            hub_to_dir_map[hub] = {}

    output = {}
    for entry in processed:
        dir_name = entry["user"]  # This is the directory name
        hub = entry["hub"]
        date = entry["date"]
        value = entry["value"]
        # Map directory name to username if possible
        username = hub_to_dir_map.get(hub, {}).get(dir_name, dir_name)
        output[(username, hub, date)] = output.get((username, hub, date), 0.0) + value

    # Hubs & dates that truly exist in the data
    hubs_with_data = {entry["hub"] for entry in processed}
    dates_with_data = {entry["date"] for entry in processed}

    # Compute per-hub totals (all_users)
    for hub in hubs_with_data:
        for date in dates_with_data:
            hub_total = sum(
                output.get((user, hub, date), 0.0)
                for (user, h, d) in output.keys()
                if h == hub and d == date and user != "all_users"
            )
            output[("all_users", hub, date)] = hub_total

    # Compute filestore totals (all_hubs)
    for filestore, hubs in FILESTORE_TO_HUBS_MAPPING.items():
        for date in dates_with_data:
            filestore_total = sum(
                output.get(("all_users", hub, date), 0.0)
                for hub in hubs
            )
            output[("all_hubs", filestore, date)] = filestore_total

    logger.info(f"[filestore_usage_all_hubs] Finished processing. Output has {len(output)} entries.")

    return output


def filestore_usage_per_hub(date_range: DateRange, hub_name: str):
    """
    Efficiently get filestore usage for a specific hub by batching/caching at the all-hubs level.
    """
    filestore = HUB_TO_FILESTORE_MAPPLING.get(hub_name)
    if not filestore:
        return {}
    all_usage = filestore_usage_all_hubs(date_range)
    # Only keep entries for this hub or for filestore totals
    filtered = {(user, hub, date): value for (user, hub, date), value in all_usage.items()
                if hub == hub_name or (user == "all_hubs" and hub == filestore)}
    return filtered


def _process_response(
    response: requests.Response,
    component_name: str,
) -> dict:
    """
    Process the response from the Prometheus server to extract absolute usage data.

    Converts the time series data into a list of usage records, then pivots by date
    and sums the absolute usage values across time steps within each date.

    """
    result = []
    for data in response["data"]["result"]:
        hub = data["metric"]["namespace"]
        user = data["metric"]["username"]

        date = [
            datetime.fromtimestamp(value[0], tz=timezone.utc).strftime("%Y-%m-%d")
            for value in data["values"]
        ]
        usage = [float(value[1]) for value in data["values"]]
        result.append(
            {
                "hub": hub,
                "component": component_name,
                "user": user,
                "date": date,
                "value": usage,
            }
        )
    pivoted_result = _pivot_response_dict(result)
    processed_result = _sum_absolute_usage_by_date(pivoted_result)
    return processed_result


def _pivot_response_dict(result: list[dict]) -> list[dict]:
    """
    Pivot the response dictionary to have top-level keys as dates.
    """
    pivot = []
    for entry in result:
        for date, value in zip(entry["date"], entry["value"]):
            pivot.append(
                {
                    "date": date,
                    "user": entry["user"],
                    "hub": entry["hub"],
                    "component": entry["component"],
                    "value": value,
                }
            )
    return pivot


def _sum_absolute_usage_by_date(result: list[dict]) -> list[dict]:
    """
    Sum the absolute usage values by date.

    The Prometheus queries can return multiple absolute usage values per day.
    We sum across all entries within each date to get the total daily usage for each user.
    """
    sums = defaultdict(float)

    for entry in result:
        key = (
            entry["date"],
            entry["user"],
            entry["hub"],
            entry["component"],
        )
        sums[key] += entry["value"]

    output = [
        {
            "date": date,
            "user": user,
            "hub": hub,
            "component": component,
            "value": total,
        }
        for (date, user, hub, component), total in sums.items()
    ]

    return output


def query_users_with_multiple_groups(
    hub_name: str | None = None,
    user_name: str | None = None,
) -> List[Dict]:
    """
    Return users who belong to more than one group.
    """
    results = []

    hubs_to_query = [hub_name] if hub_name else HUBS

    for hub in hubs_to_query:
        table_name = DB_TABLE_PREFIX + hub.replace("-", "_") 
        users = get_users_in_multiple_groups_in_database(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, table_name, user_name)
        for u in users:      
            results.append({
                "name": u,
                "hub": hub
            })

    return results


def query_users_with_no_groups(
    hub_name: str | None = None,
    user_name: str | None = None,
) -> List[Dict]:
    """
    Return users who belong to no groups.
    """
    results = []

    hubs_to_query = [hub_name] if hub_name else HUBS

    for hub in hubs_to_query:
        table_name = DB_TABLE_PREFIX + hub.replace("-", "_") 
        users = get_users_in_no_groups_in_database(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, table_name, user_name)
        for u in users:      
            results.append({
                "name": u,
                "hub": hub
            })

    return results



def get_users_from_database(dbhost,
    dbport,
    dbname,
    dbuser,
    dbpassword,
    table_name,
    username):
    """
    Query a specific user from a PostgreSQL table.
    Returns a dictionary representing the row, or None if not found.
    """
    try:
        # Connect to PostgreSQL
        conn = connect_to_postgres(dbname, dbuser, dbpassword, dbhost, dbport)
        cursor = conn.cursor()
        if username:
            cursor.execute(
                f"SELECT * FROM {table_name} WHERE username = %s;",
                (username,)
            )
            row = cursor.fetchone()
        else:
            cursor.execute(
                f"SELECT * FROM {table_name};"
            )
            row = cursor.fetchall()

        cursor.close()
        conn.close()
        return row  
    except Exception as e:
        logger.error(f"Error querying user {username}: {e}")
        return None # Row is a tuple of column values ('yijunge', 'yijunge', ['course::1524699::group::all-admins', 'course::1524699::enrollment_type::teacher', 'course::1524699::enrollment_type::student', 'course::1524699'], datetime.datetime(2025, 12, 9, 1, 9, 13, 575656))


def get_users_in_multiple_groups_in_database(dbhost,
    dbport,
    dbname,
    dbuser,
    dbpassword,
    table_name,
    username=None):
    """
    Return all rows where the groups JSONB array has more than one element.
    """
    try:
        conn = connect_to_postgres(dbname, dbuser, dbpassword, dbhost, dbport)
        cursor = conn.cursor()
        if username:
            cursor.execute(
                f"""
                SELECT username
                FROM {table_name}
                WHERE jsonb_array_length(groups) > 1
                  AND username = %s;
                """,
                (username,)
            )
        else:
            cursor.execute(
                f"""
                SELECT username
                FROM {table_name}
                WHERE jsonb_array_length(groups) > 1;
                """
            )

        rows = cursor.fetchall()

        cursor.close()
        conn.close()
        return [row[0] for row in rows]

    except Exception as e:
        logger.error(f"Error querying users with multiple groups: {e}")
        return []


def get_users_in_no_groups_in_database(dbhost,
    dbport,
    dbname,
    dbuser,
    dbpassword,
    table_name,
    username=None):
    """
    Return a list of usernames (as strings) whose groups JSONB field
    is empty ([]) or NULL.
    """
    try:
        conn = connect_to_postgres(dbname, dbuser, dbpassword, dbhost, dbport)
        cursor = conn.cursor()
        if username:
            cursor.execute(
                f"""
                SELECT username
                FROM {table_name}
                WHERE (groups IS NULL
                OR jsonb_array_length(groups) = 0)
                  AND username = %s;
                """,
                (username,)
            )
        else:
            cursor.execute(
                f"""
                SELECT username
                FROM {table_name}
                WHERE groups IS NULL
                OR jsonb_array_length(groups) = 0;
                """
            )

        rows = cursor.fetchall()  # rows like [("alice",), ("bob",)]

        cursor.close()
        conn.close()

        # Convert list of 1-element tuples into list of strings
        return [row[0] for row in rows]

    except Exception as e:
        logger.error(f"Error querying users with no groups: {e}")
        return []


@ttl_lru_cache(seconds_to_live=3600)
def get_dir_to_username_map(dbhost, dbport, dbname, dbuser, dbpassword, table_name):
    """
    Fetch all dir_name -> username mappings for a hub in one query.
    """

    try:
        conn = connect_to_postgres(dbname, dbuser, dbpassword, dbhost, dbport)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT dir_name, username FROM {table_name};"
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {row[0]: row[1] for row in rows}
    except Exception as e:
        logger.error(f"Error fetching dir_name to username map: {e}")
        return {}


def get_username_by_dir_name(dbhost,
                             dbport,
                             dbname,
                             dbuser,
                             dbpassword,
                             table_name,
                             dir_name):
    """
    Return the username corresponding to a given dir_name.
    Returns None if no matching row is found.
    """
    # Use the cached mapping instead of querying per call
    dir_to_user = get_dir_to_username_map(dbhost, dbport, dbname, dbuser, dbpassword, table_name)
    return dir_to_user.get(dir_name)


def connect_to_postgres(db_name, db_user, db_password, db_host, db_port):
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    return conn