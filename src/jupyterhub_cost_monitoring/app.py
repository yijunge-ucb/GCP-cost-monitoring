from datetime import timedelta

from fastapi import FastAPI, Query

from .const_usage import USAGE_MAP
from .date_utils import get_now_date, parse_from_to_in_query_params
from .logs import get_logger

from .query_cost_gcp import (
    query_total_costs as gcp_query_total_costs,
    query_total_costs_per_group as gcp_query_total_costs_per_group,
    query_total_costs_per_hub_per_component as gcp_query_total_costs_per_hub_per_component,
    query_total_costs_per_user as gcp_query_total_costs_per_user,
    query_costs_per_filestore as gcp_query_costs_per_filestore, 
    query_total_costs_per_nodepool as gcp_query_total_costs_per_nodepool,
    query_total_filestore_costs_per_hub as gcp_query_total_filestore_costs_per_hub,
)
from .query_usage import (
    query_users_with_multiple_groups,
    query_users_with_no_groups,
)

from .query_metadata_gcp import query_nodepool_names as gcp_query_hub_names, query_component_names as gcp_query_component_names

app = FastAPI()
logger = get_logger(__name__)


@app.get("/")
def index():
    return {"message": "Welcome to the JupyterHub Cost Monitoring API"}


@app.get("/health/ready")
def ready():
    """
    Readiness probe endpoint.
    """
    return ("200: OK", 200)


@app.get("/hub-names")
def hub_names(
    from_date: str | None = Query(
        None, alias="from", description="Start date in YYYY-MM-DDTHH:MMZ format"
    ),
    to_date: str | None = Query(
        None, alias="to", description="End date in YYYY-MM-DDTHH:MMZ format"
    ),
):
    """
    Endpoint to query hub names.
    """
    # Parse and validate date parameters into DateRange object
    date_range = parse_from_to_in_query_params(from_date, to_date)
    return gcp_query_hub_names(date_range)
    


@app.get("/component-names")
def component_names():
    """
    Endpoint to serve component names.
    """

    return gcp_query_component_names()
    


@app.get("/total-costs")
def total_costs(
    from_date: str | None = Query(
        None, alias="from", description="Start date in YYYY-MM-DDTHH:MMZ format"
    ),
    to_date: str | None = Query(
        None, alias="to", description="End date in YYYY-MM-DDTHH:MMZ format"
    ),
):
    """
    Endpoint to query total costs.
    """
    # Parse and validate date parameters into DateRange object
    date_range = parse_from_to_in_query_params(from_date, to_date)

    return gcp_query_total_costs(date_range)



@app.get("/users-with-multiple-groups")
def users_with_multiple_groups(
    hub_name: str | None = Query(None, description="Name of the hub to filter results"),
    user_name: str | None = Query(
        None, description="Name of the user to filter results"
    ),
):
    """
    Endpoint to serve users with multiple groups.
    """

    return query_users_with_multiple_groups(hub_name, user_name)


@app.get("/users-with-no-groups")
def users_with_no_groups(
    hub_name: str | None = Query(None, description="Name of the hub to filter results"),
    user_name: str | None = Query(
        None, description="Name of the user to filter results"
    ),
):
    """
    Endpoint to serve users with no groups.
    """

    return query_users_with_no_groups(hub_name, user_name)


@app.get("/total-costs-per-node-pool")
def total_costs_per_node_pool(
    from_date: str | None = Query(
        None, alias="from", description="Start date in YYYY-MM-DDTHH:MMZ format"
    ),
    to_date: str | None = Query(
        None, alias="to", description="End date in YYYY-MM-DDTHH:MMZ format"
    ),
):
    """
    Endpoint to query total costs per hub.
    """
    # Parse and validate date parameters into DateRange object
    date_range = parse_from_to_in_query_params(from_date, to_date)

    return gcp_query_total_costs_per_nodepool(date_range)


@app.get("/total-storage-cost-per-hub")
def total_storage_cost_per_hub(
    from_date: str | None = Query(
        None, alias="from", description="Start date in YYYY-MM-DDTHH:MMZ format"
    ),
    to_date: str | None = Query(
        None, alias="to", description="End date in YYYY-MM-DDTHH:MMZ format"
    ),
    hub_name: str | None = Query(None, description="Name of the hub to filter results"
    ),
):
    """
    Endpoint to query total filestore costs per hub.
    """
    date_range = parse_from_to_in_query_params(from_date, to_date)
    return gcp_query_total_filestore_costs_per_hub(date_range, hub_name)


@app.get("/filestore-costs")
def storage_costs_per_filestore(
    from_date: str | None = Query(
        None, alias="from", description="Start date in YYYY-MM-DDTHH:MMZ format"
    ),
    to_date: str | None = Query(
        None, alias="to", description="End date in YYYY-MM-DDTHH:MMZ format"
    ),
    filestore: str | None = Query(
        None, description="Filestore label to filter results"
    ),
):
    """
    Endpoint to query storage costs per Filestore instance.
    """
    # Parse and validate date parameters into DateRange object
    date_range = parse_from_to_in_query_params(from_date, to_date)
    if not filestore or filestore.lower() == "all":
        filestore = None

    return gcp_query_costs_per_filestore(date_range, filestore)


@app.get("/total-costs-per-hub-per-component")
def total_costs_per_component(
    from_date: str | None = Query(
        None, alias="from", description="Start date in YYYY-MM-DDTHH:MMZ format"
    ),
    to_date: str | None = Query(
        None, alias="to", description="End date in YYYY-MM-DDTHH:MMZ format"
    ),
    hub: str | None = Query(None, description="Name of the hub to filter results"),
    component: str | None = Query(
        None, description="Name of the component to filter results"
    ),
):
    """
    Endpoint to query total costs per component.
    """
    # Parse and validate date parameters into DateRange object
    date_range = parse_from_to_in_query_params(from_date, to_date)

    if not hub or hub.lower() == "all":
        hub = None
    if not component or component.lower() == "all":
        component = None

    return gcp_query_total_costs_per_hub_per_component(date_range, hub, component)
    

@app.get("/total-costs-per-group")
def total_costs_per_group(
    from_date: str | None = Query(
        None, alias="from", description="Start date in YYYY-MM-DDTHH:MMZ format"
    ),
    to_date: str | None = Query(
        None, alias="to", description="End date in YYYY-MM-DDTHH:MMZ format"
    ),
    hub: str | None = Query(None, description="Name of the hub to filter results"
    ),                     
):
    """
    Endpoint to query total costs per user group.
    """
    # Parse and validate date parameters into DateRange object
    date_range = parse_from_to_in_query_params(from_date, to_date)

    return gcp_query_total_costs_per_group(date_range, hub)
    


@app.get("/costs-per-user")
def costs_per_user(
    from_date: str | None = Query(
        None, alias="from", description="Start date in YYYY-MM-DDTHH:MMZ format"
    ),
    to_date: str | None = Query(
        None, alias="to", description="End date in YYYY-MM-DDTHH:MMZ format"
    ),
    hub: str | None = Query(None, description="Name of the hub to filter results"),
    component: str | None = Query(
        None, description="Name of the component to filter results"
    ),
    user: str | None = Query(None, description="Name of the user to filter results"),
    usergroup: str | None = Query(
        None, description="Name of user group to filter results"
    ),
    limit: str | None = Query(
        None, description="Limit number of results to top N users by total cost."
    ),
):
    """
    Endpoint to query costs per user by combining AWS costs with Prometheus usage data.

    This endpoint calculates individual user costs by:
    1. Getting total AWS costs per component (compute, storage) from Cost Explorer
    2. Getting usage fractions per user from Prometheus metrics
    3. Multiplying total costs by each user's usage fraction

    Query Parameters:
        from (str): Start date in YYYY-MM-DD format (defaults to 30 days ago)
        to (str): End date in YYYY-MM-DD format (defaults to current date)
        hub (str, optional): Filter to specific hub namespace
        component (str, optional): Filter to specific component (compute, filestore)
        user (str, optional): Filter to specific user
        usergroup (str, optional): Filter to specific user group
        limit (int, optional): Limit number of results to top N users by total cost.

    Returns:
        List of dicts with keys: date, hub, component, user, value (cost in USD)
        Results are sorted by date, hub, component, then value (highest cost first)
    """
    # Parse and validate date parameters into DateRange object
    date_range = parse_from_to_in_query_params(from_date, to_date)
    if usergroup:
        usergroup = usergroup.strip("{}").split(",")

    if not hub or hub.lower() == "all":
        hub = None
    if not component or component.lower() == "all":
        component = None
    if not user or user.lower() == "all":
        user = None
    if not limit or (str(limit).lower() == "all"):
        limit = None
    if not usergroup or ("all" in [u.lower() for u in usergroup]):
        usergroup = [None]

    results = []
    for ug in usergroup:

        per_user_costs = gcp_query_total_costs_per_user(
            date_range, hub, component, user, ug, limit
        )

        results.extend(per_user_costs)

    return results


