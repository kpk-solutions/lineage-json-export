import pandas as pd

# Step 1: Create DataFrame with multiple related jobs
data = {
    'Group_level': [-4, -3, -1, 3, 4, 5],
    'Jobname': ['Pred03', 'Pred04', 'Abc01', 'Abc01', 'Sucr01', 'Sucr02'],
    'Relatedpredjobs': [
        'Pred02,Pred10,Pred11',
        'Pred03,Pred13,Pred14',
        'Pred04,Pred15,Pred16',
        'Sucr01',
        'Sucr02',
        'Sucr03'
    ]
}
df = pd.DataFrame(data)

# Step 2: Clean & Normalize Related Jobs (convert string to list)
df['Relatedpredjobs'] = df['Relatedpredjobs'].apply(lambda x: [job.strip() for job in x.split(',')])

# Step 3: Separate ascendant and descendant relations
asc_df = df[df['Group_level'] < 0]
desc_df = df[df['Group_level'] > 0]

# Step 4: Lineage Logic
def build_lineage(root):
    visited = {}

    def ensure_node(job, level):
        if job not in visited:
            visited[job] = {'id': job, 'level': level, 'asc': [], 'desc': []}
        else:
            # Update level if closer to root
            if abs(level) < abs(visited[job]['level']):
                visited[job]['level'] = level

    def explore_ascendants(job, level):
        matches = asc_df[asc_df['Jobname'] == job]
        for _, row in matches.iterrows():
            for pred in row['Relatedpredjobs']:
                ensure_node(pred, level - 1)
                if pred not in visited[job]['asc']:
                    visited[job]['asc'].append(pred)
                if job not in visited[pred]['desc']:
                    visited[pred]['desc'].append(job)
                explore_ascendants(pred, level - 1)

    def explore_descendants(job, level):
        matches = desc_df[desc_df['Jobname'] == job]
        for _, row in matches.iterrows():
            for succ in row['Relatedpredjobs']:
                ensure_node(succ, level + 1)
                if succ not in visited[job]['desc']:
                    visited[job]['desc'].append(succ)
                if job not in visited[succ]['asc']:
                    visited[succ]['asc'].append(job)
                explore_descendants(succ, level + 1)

    # Start with root
    ensure_node(root, 0)
    explore_ascendants(root, 0)
    explore_descendants(root, 0)

    # Final output
    nodes = sorted(visited.values(), key=lambda x: x['level'])
    return {'root': root, 'nodes': nodes}

# Step 5: Run
root_job = 'Abc01'
output = build_lineage(root_job)

# Step 6: Display
from pprint import pprint
pprint(output)

"""
{'root': 'Abc01',
 'nodes': [
  {'id': 'Pred02', 'level': -3, 'asc': [], 'desc': ['Pred03']},
  {'id': 'Pred10', 'level': -3, 'asc': [], 'desc': ['Pred03']},
  {'id': 'Pred11', 'level': -3, 'asc': [], 'desc': ['Pred03']},
  {'id': 'Pred03', 'level': -2, 'asc': ['Pred02', 'Pred10', 'Pred11'], 'desc': ['Pred04']},
  {'id': 'Pred13', 'level': -2, 'asc': [], 'desc': ['Pred04']},
  {'id': 'Pred14', 'level': -2, 'asc': [], 'desc': ['Pred04']},
  {'id': 'Pred04', 'level': -1, 'asc': ['Pred03', 'Pred13', 'Pred14'], 'desc': ['Abc01']},
  {'id': 'Pred15', 'level': -1, 'asc': [], 'desc': ['Abc01']},
  {'id': 'Pred16', 'level': -1, 'asc': [], 'desc': ['Abc01']},
  {'id': 'Abc01', 'level': 0, 'asc': ['Pred04', 'Pred15', 'Pred16'], 'desc': ['Sucr01']},
  {'id': 'Sucr01', 'level': 1, 'asc': ['Abc01'], 'desc': ['Sucr02']},
  {'id': 'Sucr02', 'level': 2, 'asc': ['Sucr01'], 'desc': ['Sucr03']},
  {'id': 'Sucr03', 'level': 3, 'asc': ['Sucr02'], 'desc': []}
 ]}
"""
