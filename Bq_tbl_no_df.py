# lineage_bigquery.py 
#pip install google-cloud-bigquery
#pip install google-auth


from google.cloud import bigquery
from google.oauth2 import service_account
from collections import defaultdict

# Replace this path with your service account JSON file path
KEY_PATH = "/path/to/your-service-account-key.json"

# Replace with your BigQuery dataset and table
DATASET = "your_dataset"
TABLE = "your_table"

# Authenticate and create BigQuery client
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Query to get parent-child relationships
QUERY = f"""
SELECT child_job, parent_job
FROM `{DATASET}.{TABLE}`
WHERE child_job IS NOT NULL
"""

# Use BigQuery iterator (efficient for large data)
query_job = client.query(QUERY)
result_iterator = query_job.result(page_size=10000)

# Build parent-child and child-parent mappings
child_to_parents = defaultdict(list)
parent_to_children = defaultdict(list)

for row in result_iterator:
    child = row["child_job"]
    parent = row["parent_job"]
    if parent:
        child_to_parents[child].append(parent)
        parent_to_children[parent].append(child)

# Recursive lineage finder
def get_lineage(root_job):
    visited = set()
    successors = set()
    predecessors = set()

    def dfs_forward(job):
        for child in parent_to_children.get(job, []):
            if child not in visited:
                visited.add(child)
                successors.add(child)
                dfs_forward(child)

    def dfs_backward(job):
        for parent in child_to_parents.get(job, []):
            if parent not in visited:
                visited.add(parent)
                predecessors.add(parent)
                dfs_backward(parent)

    visited.clear()
    dfs_forward(root_job)

    visited.clear()
    dfs_backward(root_job)

    return {
        "root": root_job,
        "predecessors": list(predecessors),
        "successors": list(successors)
    }

# Example: Get lineage for job6
if __name__ == "__main__":
    root_job = "job6"
    result = get_lineage(root_job)
    print(result)
