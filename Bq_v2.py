from collections import defaultdict, deque
import json

def build_lineage_graph(data, root):
    parent_to_children = defaultdict(set)
    child_to_parents = defaultdict(set)

    # Build graph from raw table data
    for parent, child in data:
        if parent and parent != 'NA':
            parent_to_children[parent].add(child)
            child_to_parents[child].add(parent)

    lineage = {
        'root': root,
        'Nodes': []
    }

    visited = set()

    def process_node(node, level, direction):
        if node in visited:
            return
        visited.add(node)

        ascendants = list(child_to_parents[node]) if direction in ('asc', 'both') else []
        descendants = list(parent_to_children[node]) if direction in ('desc', 'both') else []

        lineage['Nodes'].append({
            'id': node,
            'Ascendants': ascendants,
            'Descendants': descendants,
            'Level': level
        })

        # Recursively go upstream
        if direction in ('asc', 'both'):
            for parent in ascendants:
                process_node(parent, level - 1, 'asc')

        # Recursively go downstream
        if direction in ('desc', 'both'):
            for child in descendants:
                process_node(child, level + 1, 'desc')

    # Start traversal from root
    process_node(root, 0, 'both')

    return lineage


# 👇 Example data, you can replace this with BQ or CSV input
sample_data = [
    ("P1", "K"),
    ("P2", "K"),
    ("P3", "K"),
    ("NA", "C1"),
    ("NA", "C2"),
    ("NA", "C3"),
    ("K", "C1"),
    ("K", "C2"),
    ("K", "C3"),
    ("K", "C4"),
    ("P6", "C5"),
    ("P7", "P1"),
    ("P8", "P2"),
    ("P9", "P3"),
    ("C1", "C6"),
    ("C2", "C7"),
    ("C3", "C8"),
    ("P10", "P1"),
    ("P11", "P2"),
    ("P12", "P3"),
    ("C6", "C10"),
    ("C7", "C11"),
    ("C8", "C12")
]

# Set root node
root_node = "K"

# Run the lineage generator
lineage_json = build_lineage_graph(sample_data, root_node)

# Pretty print JSON
print(json.dumps(lineage_json, indent=2))

#bq logic
from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT Parent_table, Child_table
FROM `your_project.your_dataset.your_table`
WHERE Search_table = 'K'
"""

query_job = client.query(query)
rows = query_job.result()

sample_data = [(row["Parent_table"], row["Child_table"]) for row in rows]
