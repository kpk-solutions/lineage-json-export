from collections import defaultdict, deque

# Input table data
table_data = [
    (-4, 'Pred03', 'Pred02'),
    (-3, 'Pred04', 'Pred03'),
    (-1, 'Abc01', 'Pred04'),
    (3, 'Abc01', 'Sucr01'),
    (4, 'Sucr01', 'Sucr02'),
    (5, 'Sucr02', 'Sucr03')
]

# Step 1: Build lineage maps
asc_map = defaultdict(list)   # job -> list of predecessors
desc_map = defaultdict(list)  # job -> list of successors

for level, job, rel_job in table_data:
    if level < 0:
        asc_map[job].append(rel_job)
        desc_map[rel_job].append(job)
    else:
        asc_map[rel_job].append(job)
        desc_map[job].append(rel_job)

# Step 2: Traverse lineage (BFS both ways with proper level handling)
def traverse_lineage(root):
    visited = {}
    queue = deque()
    queue.append((root, 0))

    while queue:
        job, level = queue.popleft()
        if job in visited:
            # Keep the lowest (earliest) level for predecessors,
            # and the highest (deepest) level for successors
            if level < visited[job]:
                visited[job] = level
            elif level > visited[job]:
                visited[job] = level
            else:
                continue  # already visited at same level
        else:
            visited[job] = level

        # Traverse predecessors
        for pred in asc_map.get(job, []):
            if pred not in visited or level - 1 != visited[pred]:
                queue.append((pred, level - 1))

        # Traverse successors
        for succ in desc_map.get(job, []):
            if succ not in visited or level + 1 != visited[succ]:
                queue.append((succ, level + 1))

    # Build final node list
    result = []
    for job, level in visited.items():
        result.append({
            'id': job,
            'level': level,
            'asc': asc_map.get(job, []),
            'desc': desc_map.get(job, [])
        })
    return result

# Step 3: Run for root job
root_job = 'Abc01'
nodes = traverse_lineage(root_job)

# Step 4: Create final output
final_output = {
    'root': root_job,
    'nodes': sorted(nodes, key=lambda x: x['level'])
}

# Step 5: Print the result
from pprint import pprint
pprint(final_output)
