from collections import defaultdict, deque

# Input table data (your format)
table_data = [
    (-4, 'Pred03', 'Pred02'),
    (-3, 'Pred04', 'Pred03'),
    (-1, 'Abc01', 'Pred04'),
    (3, 'Abc01', 'Sucr01'),
    (4, 'Sucr01', 'Sucr02'),
    (5, 'Sucr02', 'Sucr03')
]

# Step 1: Build maps
asc_map = defaultdict(list)  # job -> list of predecessors
desc_map = defaultdict(list)  # job -> list of successors

for level, job, rel_job in table_data:
    if level < 0:
        asc_map[job].append(rel_job)
        desc_map[rel_job].append(job)
    else:
        asc_map[rel_job].append(job)
        desc_map[job].append(rel_job)

# Step 2: Traverse both directions from root
def traverse_lineage(root):
    visited = {}
    result = []

    # Traverse backward (predecessors)
    queue = deque([(root, 0)])
    while queue:
        job, level = queue.popleft()
        if job in visited:
            continue
        visited[job] = level
        for pred in asc_map.get(job, []):
            queue.append((pred, level - 1))

    # Traverse forward (successors)
    queue = deque([(root, 0)])
    while queue:
        job, level = queue.popleft()
        if job in visited and level <= visited[job]:
            continue
        visited[job] = level
        for succ in desc_map.get(job, []):
            queue.append((succ, level + 1))

    # Build result
    for job, level in visited.items():
        result.append({
            'id': job,
            'level': level,
            'asc': asc_map.get(job, []),
            'desc': desc_map.get(job, [])
        })

    return result

# Step 3: Generate final lineage dictionary
root_job = 'Abc01'
nodes = traverse_lineage(root_job)

final_output = {
    'root': root_job,
    'nodes': sorted(nodes, key=lambda x: x['level'])
}

# Step 4: Print result
from pprint import pprint
pprint(final_output)


"""
#!/bin/bash

# Check if the mandatory argument is provided
if [ -z "$1" ]; then
  echo "Usage: ./run.sh <mandatory> [optional]"
  exit 1
fi

MANDATORY_ARG="$1"
OPTIONAL_ARG="$2"

# Call the Python script with the provided arguments
if [ -z "$OPTIONAL_ARG" ]; then
  python3 your_script.py "$MANDATORY_ARG"
else
  python3 your_script.py "$MANDATORY_ARG" --optional "$OPTIONAL_ARG"
fi
"""
