import pandas as pd
import json

# Sample lineage data (replace with your DB read)
data = {
    'job_name': ['job1', 'job1', 'job2', 'job3'],
    'predecessor': ['job2', 'job3', 'job4', 'job5'],
    'successor': ['job4', 'job5', 'job1', 'job1'],
    'level': [1, 1, 2, 2]
}

df = pd.DataFrame(data)

# Get all unique job names including those only in predecessor/successor
from collections import defaultdict

# Assuming df is your main DataFrame
pre_map = defaultdict(set)
suc_map = defaultdict(set)
level_map = {}

for _, row in df.iterrows():
    job = row['job_name']
    pre_map[job].add(row['predecessor'])
    suc_map[job].add(row['successor'])
    level_map[job] = row['level']

all_jobs = set(df['job_name']).union(df['predecessor']).union(df['successor'])

json_output = []
for job in all_jobs:
    json_output.append({
        'id': job,
        'node': job,
        'predecessor': list(pre_map.get(job, [])),
        'successor': list(suc_map.get(job, [])),
        'level': level_map.get(job)
    })


# Output as JSON
json_str = json.dumps(json_output, indent=2)
print(json_str)
