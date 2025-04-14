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
all_jobs = set(df['job_name']).union(set(df['predecessor'])).union(set(df['successor']))

# Build lookup maps for predecessor and successor
predecessor_map = df.groupby('job_name')['predecessor'].apply(set).to_dict()
successor_map = df.groupby('job_name')['successor'].apply(set).to_dict()
level_map = df.groupby('job_name')['level'].first().to_dict()

# Fill missing jobs with empty sets/defaults
json_output = []

for job in all_jobs:
    predecessors = list(predecessor_map.get(job, set()))
    successors = list(successor_map.get(job, set()))
    level = level_map.get(job, None)  # Might not be in level map

    json_output.append({
        'id': job,
        'node': job,
        'predecessor': predecessors,
        'successor': successors,
        'level': level
    })

# Output as JSON
json_str = json.dumps(json_output, indent=2)
print(json_str)
