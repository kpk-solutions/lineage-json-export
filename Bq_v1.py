from google.cloud import bigquery
from google.oauth2 import service_account
from collections import defaultdict, deque

# === 1. Replace with your actual values ===
KEY_PATH = "/path/to/your-service-account.json"
PROJECT_ID = "your_project_id"
DATASET = "your_dataset"
TABLE = "your_table"
ROOT_JOB = "Job5"

# === 2. Setup BigQuery client ===
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# === 3. Query with optimized filter ===
query = f"""
SELECT parent_table, child_table, impacttype, level
FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
WHERE NOT (impacttype = 'Self' AND level = 1 AND child_table != '{ROOT_JOB}')
"""
results = client.query(query).result()

# === 4. Build lineage maps ===
parent_to_child = defaultdict(list)
child_to_parent = defaultdict(list)

for row in results:
    parent, child = row["parent_table"], row["child_table"]
    if parent: parent_to_child[parent].append(child)
    if child: child_to_parent[child].append(parent)

# === 5. Traverse lineage ===
nodes = {}
visited = set()

def bfs(start, direction):
    queue = deque([(start, 0)])
    while queue:
        current, level = queue.popleft()
        if current in visited:
            continue
        visited.add(current)

        preds = child_to_parent.get(current, [])
        succs = parent_to_child.get(current, [])

        if current not in nodes:
            nodes[current] = {
                "id": current,
                "Successor": succs if direction == "forward" else [],
                "Predecessor": preds if direction == "backward" else [],
                "Level": level if direction == "forward" else -level
            }

        for next_node in (succs if direction == "forward" else preds):
            queue.append((next_node, level + 1))

# === 6. Build both directions ===
visited.clear()
bfs(ROOT_JOB, "forward")
bfs(ROOT_JOB, "backward")

# === 7. Final output ===
lineage_output = {
    "root": ROOT_JOB,
    "Nodes": list(nodes.values())
}

print(lineage_output)
