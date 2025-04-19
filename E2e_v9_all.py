import json
import argparse
import pandas as pd
from collections import defaultdict, deque

# Step 1: Input DataFrame
data = {
    'grouped_level': [-4, -3, -1, 3, 4, 5],
    'jobname': ['Pred03', 'Pred04', 'Abc01', 'Abc01', 'Sucr01', 'Sucr02'],
    'relatedpredjobs': [
        'Pred02,Pred10,Pred11',
        'Pred03,Pred13,Pred14',
        'Pred04,Pred15,Pred16',
        'Sucr01',
        'Sucr02',
        'Sucr03'
    ]
}
df = pd.DataFrame(data)

# Step 2: Normalize relatedpredjobs column
df['relatedpredjobs'] = df['relatedpredjobs'].apply(lambda x: [j.strip() for j in x.split(',')])

# Step 3: Build graph structures
asc_graph = defaultdict(list)
desc_graph = defaultdict(list)

for _, row in df.iterrows():
    job = row['jobname']
    for related in row['relatedpredjobs']:
        if row['grouped_level'] < 0:
            asc_graph[job].append(related)
            desc_graph[related].append(job)
        else:
            desc_graph[job].append(related)
            asc_graph[related].append(job)

# Step 4: Lineage builder function with direction argument
def build_lineage(root, direction='e2e'):
    visited = {}
    visited[root] = {'id': root, 'level': 0, 'ascendants': [], 'descendants': []}

    if direction in ['e2e', 'upstream']:
        # Upstream BFS
        asc_queue = deque([(root, 0)])
        while asc_queue:
            key_job, level = asc_queue.popleft()
            for ascendant in asc_graph.get(key_job, []):
                if ascendant not in visited:
                    visited[ascendant] = {'id': ascendant, 'level': level - 1, 'ascendants': [], 'descendants': []}
                    asc_queue.append((ascendant, level - 1))
                if ascendant not in visited[key_job]['ascendants']:
                    visited[key_job]['ascendants'].append(ascendant)
                if key_job not in visited[ascendant]['descendants']:
                    visited[ascendant]['descendants'].append(key_job)

    if direction in ['e2e', 'downstream']:
        # Downstream BFS
        desc_queue = deque([(root, 0)])
        while desc_queue:
            current, level = desc_queue.popleft()
            for child in desc_graph.get(current, []):
                if child not in visited:
                    visited[child] = {'id': child, 'level': level + 1, 'ascendants': [], 'descendants': []}
                    desc_queue.append((child, level + 1))
                if child not in visited[current]['descendants']:
                    visited[current]['descendants'].append(child)
                if current not in visited[child]['ascendants']:
                    visited[child]['ascendants'].append(current)

    return {
        'root': root,
        'nodes': sorted(visited.values(), key=lambda x: x['level'])
    }

# Step 5: Main function
def full_lineage(root_job, direction='e2e'):
    """
    direction: 'e2e' (default), 'upstream', or 'downstream'
    """
    return build_lineage(root_job, direction)


# print(json.dumps(full_lineage('Abc01', direction='upstream'), indent=2))
# print(json.dumps(full_lineage('Abc01', direction='downstream'), indent=2))
print(json.dumps(full_lineage('Abc01', direction='e2e'), indent=2))

