"""
Constants used to compose queries against GCP BigQuery for GKE cost monitoring.
"""

import os

#BIGQUERY_TABLE_NAME = os.environ["BIGQUERY_TABLE_NAME"]
#CLUSTER_NAME = os.environ["CLUSTER_NAME"]

BIGQUERY_TABLE_NAME = ""
CLUSTER_NAME = ""
# Map GCP service descriptions to logical components
SERVICE_COMPONENT_MAP = {
    "Kubernetes Engine": "control-plane",
    "Compute Engine": "compute",
    "Networking": "networking",
}

# List of nodepool names considered as "core" nodepools
#CORE_NODEPOOLS = os.environ.get("CORE_NODEPOOLS", "").split(",") if os.environ.get("CORE_NODEPOOLS") else []
CORE_NODEPOOLS = [""]
NODEPOOL_NAME_KEY = ""

HUBS  = [] 

HUB_TO_NODEPOOL_MAPPLING = {}

NODEPOOL_TO_HUBS_MAPPING = {}

HUB_TO_FILESTORE_MAPPLING = {}

FILESTORE_TO_HUBS_MAPPING = {}
                             

PROMETHEUS_HOST =  ""
PROMETHEUS_PORT =  


DB_NAME = ""
DB_USER =""
DB_PASSWORD =""
DB_HOST =""
DB_PORT = ""
DB_TABLE_PREFIX = ""