"""
Queries to GCP BigQuery to get different kinds of cost data for GKE clusters.
"""

from collections import defaultdict
from google.cloud import bigquery
from .const_cost_gcp import (
    BIGQUERY_TABLE_NAME,
    CLUSTER_NAME,
    SERVICE_COMPONENT_MAP,
    CORE_NODEPOOLS,
)
from .date_utils import DateRange
from .cache import ttl_lru_cache
from .query_usage import  memory_usage_per_hub, filestore_usage_per_hub, get_users_from_database, get_dir_to_username_map
from .const_cost_gcp import HUB_TO_NODEPOOL_MAPPLING, HUB_TO_FILESTORE_MAPPLING, DB_HOST, DB_NAME, DB_PASSWORD, DB_USER, DB_PORT, DB_TABLE_PREFIX
from .logs import get_logger


logger = get_logger(__name__)

bq_client = bigquery.Client()


@ttl_lru_cache(seconds_to_live=3600)
def query_total_filestore_costs(date_range: DateRange):
    """
    Query total Filestore costs for a GKE cluster from BigQuery.

    Returns:
        [
            {"date": "YYYY-MM-DD", "cost": "12.34", "name": "account"},
            ...
        ]
    """
    from_date, to_date = date_range.gcp_range  # datetime objects

    query = f"""
    SELECT
      TIMESTAMP_TRUNC(usage_start_time, DAY) AS usage_day,
      SUM(cost) AS total_cost_usd
    FROM
      `{BIGQUERY_TABLE_NAME}`
    WHERE
      service.description = 'Cloud Filestore'
      AND EXISTS (
        SELECT 1
        FROM UNNEST(labels) AS l
        WHERE l.key = 'filestore-deployment'
      )
      AND usage_start_time >= TIMESTAMP('{from_date}')
      AND usage_start_time <  TIMESTAMP('{to_date}')
    GROUP BY
      usage_day
    ORDER BY
      usage_day ASC
    """

    results = bq_client.query(query).result()

    return [
        {
            "date": row.usage_day.strftime("%Y-%m-%d"),
            "cost": f"{row.total_cost_usd:.2f}",
            "name": "account"
        }
        for row in results
    ]


@ttl_lru_cache(seconds_to_live=3600)
def query_costs_per_filestore(date_range: DateRange, filestore_label: str):
    """
    Query total Filestore costs for a GKE cluster from BigQuery.

    Returns:
        [
            {"date": "YYYY-MM-DD", "cost": "12.34", "name": "filestore_label"},
            ...
        ]
    """
    from_date, to_date = date_range.gcp_range  # datetime objects
    
    if not filestore_label:
        query = f"""
        SELECT
            TIMESTAMP_TRUNC(usage_start_time, DAY) AS usage_day,
            l.value AS filestore_label,
            SUM(cost) AS total_cost_usd
        FROM
        `{BIGQUERY_TABLE_NAME}`,
        UNNEST(labels) AS l
        WHERE
            service.description = 'Cloud Filestore'
            AND l.key = 'filestore-deployment'
            AND usage_start_time >= TIMESTAMP('{from_date}')
            AND usage_start_time < TIMESTAMP('{to_date}')
        GROUP BY
            usage_day, filestore_label
        ORDER BY
            usage_day ASC, filestore_label ASC;
        """
        results = bq_client.query(query).result()
        output = [
            {
                "date": row.usage_day.strftime("%Y-%m-%d"),
                "cost": f"{row.total_cost_usd:.2f}",
                "name": row.filestore_label or "unknown"
            }
            for row in results
        ]
        return output

    query = f"""
    SELECT
        TIMESTAMP_TRUNC(usage_start_time, DAY) AS usage_day,
        l.value AS filestore_label,
        SUM(cost) AS total_cost_usd
    FROM
        `{BIGQUERY_TABLE_NAME}`,
        UNNEST(labels) AS l
    WHERE
        service.description = 'Cloud Filestore'
        AND l.key = 'filestore-deployment'
        AND l.value = '{filestore_label}'
        AND usage_start_time >= TIMESTAMP('{from_date}')
        AND usage_start_time < TIMESTAMP('{to_date}')
    GROUP BY
        usage_day, filestore_label
    ORDER BY
        usage_day ASC, filestore_label ASC;

    """

    results = bq_client.query(query).result()

    return [
        {
            "date": row.usage_day.strftime("%Y-%m-%d"),
            "cost": f"{row.total_cost_usd:.2f}",
            "name": row.filestore_label or "unknown"
        }
        for row in results
    ]


@ttl_lru_cache(seconds_to_live=3600)
def query_total_compute_costs(date_range: DateRange):
    """
    Query total compute costs (compute engine, kubernetes engine, and networking) for a GKE cluster from BigQuery.
    Returns list of dicts: [{date, cost, name}]
    """
    from_date, to_date = date_range.gcp_range
    query = f"""
    SELECT
      TIMESTAMP_TRUNC(usage_start_time, DAY) AS usage_day,
      SUM(cost) AS total_cost_usd
    FROM `{BIGQUERY_TABLE_NAME}`, UNNEST(labels) AS labels
    WHERE
      labels.key IN ('k8s-cluster-name', 'goog-k8s-cluster-name')
      AND labels.value = '{CLUSTER_NAME}'
      AND usage_start_time >= TIMESTAMP('{from_date}')
      AND usage_start_time <= TIMESTAMP('{to_date}')
    GROUP BY usage_day
    ORDER BY usage_day ASC
    """
    results = bq_client.query(query).result()
    return [
        {"date": row.usage_day.strftime("%Y-%m-%d"), "cost": f"{row.total_cost_usd:.2f}", "name": "account"}
        for row in results
    ]


@ttl_lru_cache(seconds_to_live=3600)
def query_total_costs(date_range: DateRange):
    """
    Combine and sum the total compute + filestore costs for each day.

    Returns:
        [
            {"date": "YYYY-MM-DD", "cost": "XX.XX", "name": "account"},
            ...
        ]
    """
    # Fetch individual cost streams
    filestore_costs = query_total_filestore_costs(date_range)
    compute_costs = query_total_compute_costs(date_range)

    # Aggregate by date
    daily_costs = defaultdict(float)

    # Add filestore
    for row in filestore_costs:
        daily_costs[row["date"]] += float(row["cost"])

    # Add compute
    for row in compute_costs:
        daily_costs[row["date"]] += float(row["cost"])

    # Produce sorted output
    return [
        {"date": date, "cost": f"{total:.2f}", "name": "account"}
        for date, total in sorted(daily_costs.items(), key=lambda x: x[0])
    ]


@ttl_lru_cache(seconds_to_live=3600)
def query_total_costs_per_nodepool(date_range: DateRange, nodepool_name: str = None):
    """
    Query total compute costs per nodepool from BigQuery.
    Returns list of dicts: [{date, cost, name}]
    """
    from_date, to_date = date_range.gcp_range
    nodepool_filter = ""
    if nodepool_name:
        nodepool_filter = f"""
        AND EXISTS (
            SELECT 1 FROM UNNEST(labels) AS l
            WHERE l.key IN ('goog-k8s-node-pool-name')
              AND l.value = '{nodepool_name}'
        )
        """
    query = f"""
    WITH filtered_rows AS (
      SELECT *,
        (SELECT l.value FROM UNNEST(labels) AS l WHERE l.key IN ('goog-k8s-node-pool-name') LIMIT 1) AS node_pool
      FROM `{BIGQUERY_TABLE_NAME}` t
      WHERE service.description in ('Compute Engine', 'Networking')
        AND EXISTS (
          SELECT 1 FROM UNNEST(t.labels) AS l
          WHERE l.key IN ('k8s-cluster-name', 'goog-k8s-cluster-name')
            AND l.value = '{CLUSTER_NAME}'
        )
        {nodepool_filter}
        AND usage_start_time >= TIMESTAMP('{from_date}')
        AND usage_start_time <= TIMESTAMP('{to_date}')
    )
    SELECT
      TIMESTAMP_TRUNC(usage_start_time, DAY) AS usage_day,
      node_pool,
      SUM(cost) AS total_cost_usd
    FROM filtered_rows
    GROUP BY usage_day, node_pool
    ORDER BY usage_day ASC
    """

    results = bq_client.query(query).result()
    return [
        {"date": row.usage_day.strftime("%Y-%m-%d"), "cost": f"{row.total_cost_usd:.2f}", "name": row.node_pool or "unknown"}
        for row in results
    ]


@ttl_lru_cache(seconds_to_live=3600)
def query_total_compute_costs_per_hub(date_range: DateRange, hub_name: str ):
    """
    Query total compute costs per hub.
    Returns list of dicts: [{date, cost, name}]
    """
    memory_usage = memory_usage_per_hub(date_range, hub_name)
    nodepool = HUB_TO_NODEPOOL_MAPPLING.get(hub_name)
    nodepool_memory_usage = {}
    hub_memory_usage = {}
    
    for (u, h, date), usage in memory_usage.items():
        if u == "all_hubs" and h == nodepool:
            nodepool_memory_usage[date] = usage
        if u == "all_users" and h == hub_name:
            hub_memory_usage[date] = usage
    
    # Compute per-day hub-to-nodepool ratio
    hub_to_nodepool_ratio = {
        date: (hub_memory_usage.get(date, 0.0) / nodepool_memory_usage.get(date, 1.0))
        for date in hub_memory_usage
    }

    nodepool_cost_rows = query_total_costs_per_nodepool(date_range, nodepool)

    result = []
    for row in nodepool_cost_rows:
        date = row["date"]
        ratio = hub_to_nodepool_ratio.get(date, 0.0)
        cost = float(row["cost"]) * ratio
        result.append({
            "date": date,
            "cost": f"{cost:.2f}",
            "name": hub_name
        })

    return result
        
@ttl_lru_cache(seconds_to_live=3600)
def query_total_filestore_costs_per_hub(date_range: DateRange, hub_name: str):
    """
    Query total filestore costs per hub.
    Returns list of dicts: [{date, cost, name}]
    """
    filestore = HUB_TO_FILESTORE_MAPPLING.get(hub_name)
    filestore_usage = filestore_usage_per_hub(date_range, hub_name)
    
    hub_usage_per_day = {
        date: usage
        for (user, hub, date), usage in filestore_usage.items()
        if user == "all_users" and hub == hub_name
    }
    filestore_usage_total_per_day = {
        date: usage
        for (user, hub, date), usage in filestore_usage.items()
        if user == "all_hubs" and hub == filestore
    }
    
    filestore_cost_rows = query_costs_per_filestore(date_range, filestore)

    result = []
    for row in filestore_cost_rows:
        date = row["date"]
        hub_usage = hub_usage_per_day.get(date, 0.0)
        total_usage = filestore_usage_total_per_day.get(date, 1.0)
        ratio = hub_usage / total_usage if total_usage else 0.0
        cost = float(row["cost"]) * ratio
        result.append({
            "date": date,
            "cost": f"{cost:.2f}",
            "name": hub_name
        })

    return result


@ttl_lru_cache(seconds_to_live=3600)
def query_total_costs_per_hub_per_component(
    date_range: DateRange,
    hub: str = None,
    component: str = None,
):
    """
    If hub=None → sum across all hubs for each component.
    If hub=<hubname> → return cost only for that hub.

    Returns:
        [
            {"date": "YYYY-MM-DD", "component": "compute", "cost": "12.34"},
            {"date": "YYYY-MM-DD", "component": "filestore", "cost": "10.00"},
            ...
        ]
    """
    if not hub:
        return query_total_costs_per_component(date_range, component=component)
    
    compute_costs_per_hub = query_total_compute_costs_per_hub(date_range, hub)
    filestore_costs_per_hub = query_total_filestore_costs_per_hub(date_range, hub)
    results = []

    # ----- COMPUTE -----
    if component in (None, "compute"):
        for row in compute_costs_per_hub:
            results.append(
                {
                    "date": row["date"],
                    "component": "compute",
                    "cost": row["cost"],
                }
            )

    # ----- FILESTORE / HOME STORAGE -----
    if component in (None, "filestore"):
        for row in filestore_costs_per_hub:
            results.append(
                {
                    "date": row["date"],
                    "component": "filestore",
                    "cost": row["cost"],
                }
            )

    # Always return sorted by date + component for consistency
    results.sort(key=lambda r: (r["date"], r["component"]))

    return results


@ttl_lru_cache(seconds_to_live=3600)
def query_total_costs_per_component(
    date_range: DateRange,
    component: str = None,
):
    """
    Query total costs per component (service) from BigQuery.
    Aggregates costs across all nodepools.
    Returns list of dicts: [{date, cost, component}]
    """
    from_date, to_date = date_range.gcp_range

    query = f"""
    SELECT
        TIMESTAMP_TRUNC(usage_start_time, DAY) AS usage_day,
        service.description AS service,
        SUM(cost) AS total_cost_usd
    FROM `{BIGQUERY_TABLE_NAME}` t
    WHERE
        EXISTS (
            SELECT 1 FROM UNNEST(t.labels) AS l
            WHERE l.key IN ('k8s-cluster-name', 'goog-k8s-cluster-name')
              AND l.value = '{CLUSTER_NAME}'
        )
        AND usage_start_time >= TIMESTAMP('{from_date}')
        AND usage_start_time <= TIMESTAMP('{to_date}')
    GROUP BY usage_day, service
    ORDER BY usage_day ASC, service ASC
    """

    results = bq_client.query(query).result()

    by_date = {}

    for row in results:
        comp = SERVICE_COMPONENT_MAP.get(row.service, "other")
        cost = float(row.total_cost_usd)

        
        date_str = row.usage_day.strftime("%Y-%m-%d")
        by_date.setdefault(date_str, {}).setdefault(comp, 0.0)
        by_date[date_str][comp] += cost

    filestore_costs = query_total_filestore_costs(date_range)

    for row in filestore_costs:
        date_str = row["date"]
        cost = float(row["cost"])
        by_date.setdefault(date_str, {}).setdefault("filestore", 0.0)
        by_date[date_str]["filestore"] += cost

    # Subtract core costs separately
    core_costs = _query_core_costs(date_range)
    for date, costs in core_costs.items():
        for comp_name in ("compute", "networking"):
            if comp_name in by_date.get(date, {}):
                by_date[date][comp_name] = max(0.0, by_date[date][comp_name] - costs.get(comp_name, 0.0))
        by_date.setdefault(date, {})["core"] = sum(costs.values())

    # Prepare output
    out = []
    for date, comps in by_date.items():
        for comp_name, cost in comps.items():
            if component and comp_name != component:
                continue
            out.append({"date": date, "cost": f"{cost:.2f}", "component": comp_name})

    return out


def _query_core_costs(date_range: DateRange):
    """
    Query core costs per day for compute and networking separately.
    Returns a dict of dicts: {date: {"compute": cost, "networking": cost}}
    """
    from_date, to_date = date_range.gcp_range

    if not CORE_NODEPOOLS:
        return {}

    nodepool_filter = ", ".join([f"'{np}'" for np in CORE_NODEPOOLS])

    query = f"""
    WITH filtered_rows AS (
      SELECT *,
        (SELECT l.value
         FROM UNNEST(labels) AS l
         WHERE l.key = 'goog-k8s-node-pool-name'
         LIMIT 1) AS node_pool
      FROM `{BIGQUERY_TABLE_NAME}` t
      WHERE service.description IN ('Compute Engine', 'Networking')
        AND EXISTS (
          SELECT 1 FROM UNNEST(t.labels) AS l
          WHERE l.key IN ('k8s-cluster-name', 'goog-k8s-cluster-name')
            AND l.value = '{CLUSTER_NAME}'
        )
        AND EXISTS (
          SELECT 1 FROM UNNEST(t.labels) AS l
          WHERE l.key = 'goog-k8s-node-pool-name'
            AND l.value IN ({nodepool_filter})
        )
        AND usage_start_time >= TIMESTAMP('{from_date}')
        AND usage_start_time <= TIMESTAMP('{to_date}')
    )
    SELECT
      TIMESTAMP_TRUNC(usage_start_time, DAY) AS usage_day,
      service.description AS service,
      SUM(cost) AS total_cost_usd
    FROM filtered_rows
    GROUP BY usage_day, service
    ORDER BY usage_day ASC
    """

    results = bq_client.query(query).result()

    core_costs = {}
    for row in results:
        date_str = row.usage_day.strftime("%Y-%m-%d")
        comp_name = "compute" if row.service == "Compute Engine" else "networking"
        core_costs.setdefault(date_str, {})[comp_name] = float(row.total_cost_usd)

    return core_costs


@ttl_lru_cache(seconds_to_live=3600)
def query_total_costs_per_user(
    date_range: DateRange,
    hub: str,
    component: str = None,
    user: str | None = None,
    usergroup: str | None = None,
    limit: int | None = None,
):
    """
    Calculate total costs per user with daily granularity.
    Computes ratio of per-user usage to total hub usage per day,
    then multiplies by hub daily costs.
    """
    if not hub:
        return []
    results = []
    cost_accumulator = defaultdict(lambda: defaultdict(float))  # {user: {date: total_cost}}

    # -------------------
    # Compute costs
    # -------------------
    if component in (None, "compute"):
        compute_costs_per_hub = query_total_compute_costs_per_hub(date_range, hub)
        memory_usage = memory_usage_per_hub(date_range, hub)
        hub_totals_per_day = {date: usage for (u, h, date), usage in memory_usage.items() if u == "all_users" and h == hub}
        per_user_memory = {(u, d): usage for (u, h, d), usage in memory_usage.items() if h == hub and u != "all_users"}

        # Apply user/usergroup/limit filters
        if user:
            per_user_memory = {(u, d): usage for (u, d), usage in per_user_memory.items() if u == user}
        if usergroup:
            table_name = DB_TABLE_PREFIX + hub.replace("-", "_")
            users_data = get_users_from_database(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, table_name, username=None)
            group_users = {u[0] for u in users_data if usergroup in u[2]}
            per_user_memory = {(u, d): usage for (u, d), usage in per_user_memory.items() if u in group_users}
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                raise ValueError(f"limit must be an integer, got {limit!r}")
            total_usage_per_user = defaultdict(float)
            for (u, _), usage in per_user_memory.items():
                total_usage_per_user[u] += usage
            top_users = set(sorted(total_usage_per_user, key=lambda u: total_usage_per_user[u], reverse=True)[:limit])
            per_user_memory = {(u, d): usage for (u, d), usage in per_user_memory.items() if u in top_users}

        for row in compute_costs_per_hub:
            date = row["date"]
            hub_total = hub_totals_per_day.get(date, 0)
            for (u, d), usage in per_user_memory.items():
                if d != date:
                    continue
                ratio = usage / hub_total if hub_total else 0
                cost = float(row["cost"]) * ratio
                if component is None:
                    cost_accumulator[u][d] += cost
                else:
                    results.append({"date": d, "component": "compute", "user": u, "cost": f"{cost:.2f}"})

    # -------------------
    # Filestore costs
    # -------------------
    if component in (None, "filestore"):
        filestore_usage = filestore_usage_per_hub(date_range, hub)
        filestore_costs_per_hub = query_total_filestore_costs_per_hub(date_range, hub)
        hub_totals_per_day = {date: usage for (u, h, date), usage in filestore_usage.items() if u == "all_users" and h == hub}
        per_user_filestore = {(u, d): usage for (u, h, d), usage in filestore_usage.items() if h == hub and u != "all_users"}

        # Apply user/usergroup/limit filters
        if user:
            per_user_filestore = {(u, d): usage for (u, d), usage in per_user_filestore.items() if u == user}
        if usergroup:
            table_name = DB_TABLE_PREFIX + hub.replace("-", "_")
            users_data = get_users_from_database(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, table_name, username=None)
            group_users = {u[0] for u in users_data if usergroup in u[2]}
            per_user_filestore = {(u, d): usage for (u, d), usage in per_user_filestore.items() if u in group_users}
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                raise ValueError(f"limit must be an integer, got {limit!r}")
            total_usage_per_user = defaultdict(float)
            for (u, _), usage in per_user_filestore.items():
                total_usage_per_user[u] += usage
            top_users = set(sorted(total_usage_per_user, key=lambda u: total_usage_per_user[u], reverse=True)[:limit])
            per_user_filestore = {(u, d): usage for (u, d), usage in per_user_filestore.items() if u in top_users}

        for row in filestore_costs_per_hub:
            date = row["date"]
            hub_total = hub_totals_per_day.get(date, 0)
            for (u, d), usage in per_user_filestore.items():
                if d != date:
                    continue
                ratio = usage / hub_total if hub_total else 0
                cost = float(row["cost"]) * ratio
                if component is None:
                    cost_accumulator[u][d] += cost
                else:
                    results.append({"date": d, "component": "filestore", "user": u, "cost": f"{cost:.2f}"})

    # Aggregate if component is None
    if component is None:
        for u, daily_costs in cost_accumulator.items():
            for date, cost in daily_costs.items():
                results.append({"date": date, "user": u, "cost": f"{cost:.2f}", "component": "all"})


    results.sort(key=lambda r: (r["date"], r["component"], r["user"]))

    return results


@ttl_lru_cache(seconds_to_live=3600)
def query_total_costs_per_group(date_range: DateRange, hub: str, component: str = None):
    """
    Calculate total costs per group per day using daily per-user costs.
    """
    if not hub:
        return []

    per_user_costs = query_total_costs_per_user(date_range, hub, component)
    table_name = DB_TABLE_PREFIX + hub.replace("-", "_")
    users_data = get_users_from_database(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, table_name, username=None)
    # Build user_to_groups mapping once
    user_to_groups = {u[0]: u[2] if u[2] else ["nogroup"] for u in users_data}

    group_cost_accumulator = defaultdict(lambda: defaultdict(float))  # {group: {date: total_cost}}

    for entry in per_user_costs:
        user = entry["user"]
        date = entry["date"]
        cost = float(entry["cost"])
        groups = user_to_groups.get(user, ["nogroup"])
        for group in groups:
            group_cost_accumulator[group][date] += cost

    # Flatten results in one pass
    results = [
        {"date": date, "usergroup": group, "cost": f"{cost:.2f}"}
        for group, daily_costs in group_cost_accumulator.items()
        for date, cost in daily_costs.items()
    ]

    results.sort(key=lambda r: (r["date"], r["usergroup"]))
    return results



