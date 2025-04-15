import pandas as pd
from collections import defaultdict, deque

# Step 1: Create DataFrame
data = {
    'Group_level': [-4, -3, -1, 3, 4, 5],
    'Jobname': ['Pred03', 'Pred04', 'Abc01', 'Abc01', 'Sucr01', 'Sucr02'],
    'Relatedpredjobs': ['Pred02', 'Pred03', 'Pred04', 'Sucr01', 'Sucr02', 'Sucr03']
}

df = pd.DataFrame(data)

# Step 2: Build lineage maps from DataFrame
asc_map = defaultdict(list)   # job -> list of predecessors
desc_map = defaultdict(list)  # job -> list of successors

for _, row in df.iterrows():
    level = row['Group_level']
    job = row['Jobname']
    rel = row['Relatedpredjobs']
    if level < 0:
        asc_map[job].append(rel)
        desc_map[rel].append(job)
    else:
        asc_map[rel].append(job)
        desc_map[job].append(rel)

# Step 3: Traverse lineage from root
def traverse_lineage(root):
    visited = {}
    queue = deque()
    queue.append((root, 0))

    while queue:
        job, level = queue.popleft()
        if job in visited:
            if level < visited[job]:
                visited[job] = level
            elif level > visited[job]:
                visited[job] = level
            else:
                continue
        else:
            visited[job] = level

        # Explore predecessors
        for pred in asc_map.get(job, []):
            if pred not in visited or visited[pred] != level - 1:
                queue.append((pred, level - 1))

        # Explore successors
        for succ in desc_map.get(job, []):
            if succ not in visited or visited[succ] != level + 1:
                queue.append((succ, level + 1))

    # Build node output
    result = []
    for job, level in visited.items():
        result.append({
            'id': job,
            'level': level,
            'asc': asc_map.get(job, []),
            'desc': desc_map.get(job, [])
        })

    return result

# Step 4: Generate output from root
root_job = 'Abc01'
nodes = traverse_lineage(root_job)

final_output = {
    'root': root_job,
    'nodes': sorted(nodes, key=lambda x: x['level'])
}

# Step 5: Print final output
from pprint import pprint
pprint(final_output)
