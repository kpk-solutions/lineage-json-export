import json
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
            # related is ascendant (predecessor)
            asc_graph[job].append(related)
            desc_graph[related].append(job)
        else:
            # related is descendant (successor)
            desc_graph[job].append(related)
            asc_graph[related].append(job)

# Step 4: Lineage builder function
def build_lineage(root):
    visited = {}
    
    # Initialize root
    visited[root] = {'id': root, 'level': 0, 'ascendants': [], 'descendants': []}

    # Upstream BFS
    asc_queue = deque([(root, 0)])
    while asc_queue:
        current, level = asc_queue.popleft()
        for parent in asc_graph.get(current, []):
            if parent not in visited:
                visited[parent] = {'id': parent, 'level': level - 1, 'ascendants': [], 'descendants': []}
                asc_queue.append((parent, level - 1))
            if parent not in visited[current]['ascendants']:
                visited[current]['ascendants'].append(parent)
            if current not in visited[parent]['descendants']:
                visited[parent]['descendants'].append(current)

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

    # Sort and return final result
    return {
        'root': root,
        'nodes': sorted(visited.values(), key=lambda x: x['level'])
    }

def full_lineage(root_job):
    # root_job = 'Abc01'
    lineage = build_lineage(root_job)
    # lineage = json.dumps(lineage)
    return lineage


"""
{
 'root': 'Abc01',
 'nodes': [
   {'id': 'Pred02', 'level': -3, 'ascendants': [], 'descendants': ['Pred03']},
   {'id': 'Pred10', 'level': -3, 'ascendants': [], 'descendants': ['Pred03']},
   {'id': 'Pred11', 'level': -3, 'ascendants': [], 'descendants': ['Pred03']},
   {'id': 'Pred03', 'level': -2, 'ascendants': ['Pred02', 'Pred10', 'Pred11'], 'descendants': ['Pred04']},
   ...
   {'id': 'Sucr03', 'level': 3, 'ascendants': ['Sucr02'], 'descendants': []}
 ]
}
"""
