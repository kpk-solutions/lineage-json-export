
from collections import defaultdict, deque

# Input data
data = [
    {"Parent": "Job5", "Child": "Job6"},
    {"Parent": "Job4", "Child": "Job7"},
    {"Parent": "Job3", "Child": "Job8"},
    {"Parent": "Job2", "Child": "Job5"},
    {"Parent": "Job1", "Child": "Job4"},
    {"Parent": "Job22", "Child": "Job3"},
    {"Parent": "Job6", "Child": "Job9"},
    {"Parent": "Job6", "Child": "Job10"},
]

# Create forward and reverse graph
forward_graph = defaultdict(list)
reverse_graph = defaultdict(list)

for row in data:
    parent, child = row["Parent"], row["Child"]
    forward_graph[parent].append(child)
    reverse_graph[child].append(parent)

# DFS to find all successors
def get_successors(job):
    visited = set()
    stack = deque([job])
    while stack:
        current = stack.pop()
        for neighbor in forward_graph.get(current, []):
            if neighbor not in visited:
                visited.add(neighbor)
                stack.append(neighbor)
    return list(visited)

# DFS to find all predecessors
def get_predecessors(job):
    visited = set()
    stack = deque([job])
    while stack:
        current = stack.pop()
        for neighbor in reverse_graph.get(current, []):
            if neighbor not in visited:
                visited.add(neighbor)
                stack.append(neighbor)
    return list(visited)

# Set root job
root_job = "Job6"

# Get lineage
lineage = {
    "Root": root_job,
    "Predecessors": get_predecessors(root_job),
    "Successors": get_successors(root_job),
}

print(lineage)
