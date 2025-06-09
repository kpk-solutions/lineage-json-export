# Install necessary libraries
# pip install pandas google-cloud-bigquery

import pandas as pd
from collections import deque, defaultdict
from google.cloud import bigquery

# Replace with your root job
ROOT_JOB = "K"

# Step 1: Load data from BigQuery
def load_data_from_bq():
    client = bigquery.Client()
    query = """
        SELECT *
        FROM `your_project.your_dataset.your_table`
        WHERE impact_type != 'self'
    """
    df = client.query(query).to_dataframe()
    return df

# Step 2: Build graph structures
def build_graphs(df):
    forward_graph = defaultdict(set)
    reverse_graph = defaultdict(set)

    for _, row in df.iterrows():
        parent = row['parentjob']
        child = row['childjob']

        forward_graph[parent].add(child)
        reverse_graph[child].add(parent)

    return forward_graph, reverse_graph

# Step 3: BFS to traverse lineage with levels
def traverse_graph(root, forward_graph, reverse_graph):
    nodes = []
    visited = set()

    queue = deque()
    queue.append((root, 0))  # Level 0 = root

    while queue:
        current, level = queue.popleft()
        if current in visited:
            continue
        visited.add(current)

        ascendants = list(reverse_graph.get(current, []))
        descendants = list(forward_graph.get(current, []))

        # Save node
        nodes.append({
            'id': current,
            'Ascendants': ascendants,
            'Descendants': descendants,
            'Level': level
        })

        # Traverse backward (upstream)
        if level <= 0:
            for asc in ascendants:
                if asc not in visited:
                    queue.append((asc, level - 1))

        # Traverse forward (downstream)
        if level >= 0:
            for desc in descendants:
                if desc not in visited:
                    queue.append((desc, level + 1))

    return nodes

# Step 4: Main execution
def generate_lineage_json():
    df = load_data_from_bq()
    
    # Optional: filter unnecessary records
    df = df[df['childjob'] != ROOT_JOB]

    forward_graph, reverse_graph = build_graphs(df)
    lineage_nodes = traverse_graph(ROOT_JOB, forward_graph, reverse_graph)

    result = {
        'root': ROOT_JOB,
        'Nodes': lineage_nodes
    }

    return result

# Example use
if __name__ == "__main__":
    lineage = generate_lineage_json()
    import json
    print(json.dumps(lineage, indent=2))
