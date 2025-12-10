"""
Metadata query functions for GCP.
"""

from .const_cost_gcp import BIGQUERY_TABLE_NAME, CLUSTER_NAME, NODEPOOL_NAME_KEY
from .cache import ttl_lru_cache
from .date_utils import DateRange
from google.cloud import bigquery

bq_client = bigquery.Client()

@ttl_lru_cache(seconds_to_live=3600)
def query_nodepool_names(date_range: DateRange):
    """
    Query nodepool names from BigQuery for GCP.
    """
    from_date, to_date = date_range.gcp_range

    query = f"""
    SELECT DISTINCT l.value AS nodepool_name
    FROM `{BIGQUERY_TABLE_NAME}` t,
         UNNEST(labels) AS l
    WHERE
      EXISTS (
          SELECT 1 FROM UNNEST(t.labels) AS l
          WHERE l.key IN ('k8s-cluster-name', 'goog-k8s-cluster-name')
            AND l.value = '{CLUSTER_NAME}'
        )
        AND l.key = '{NODEPOOL_NAME_KEY}'
      AND usage_start_time >= TIMESTAMP('{from_date}')
      AND usage_start_time <= TIMESTAMP('{to_date}')
    ORDER BY nodepool_name
    """

    results = bq_client.query(query).result()

    nodepool_names = [row.nodepool_name for row in results]
    return nodepool_names
    

def query_component_names():
    """
    Return list of component names for GCP.
    """
    from .const_usage import USAGE_MAP
    return list(USAGE_MAP.keys())
