"""
Job Lineage Extraction Script using NetworkX
Install required package:
    pip install networkx
"""

import networkx as nx

# Input data
data = [
    ("Job5", "Job6"),
    ("Job4", "Job7"),
    ("Job3", "Job8"),
    ("Job2", "Job5"),
    ("Job1", "Job4"),
    ("Job22", "Job3"),
    ("Job6", "Job9"),
    ("Job6", "Job10"),
]

# Create directed graph
G = nx.DiGraph()
G.add_edges_from(data)

root = "Job6"

# Find predecessors and successors
predecessors = list(nx.ancestors(G, root))
successors = list(nx.descendants(G, root))

# Final result
result = {
    "root": root,
    "Predecessor": predecessors,
    "Successor": successors
}

print(result)
