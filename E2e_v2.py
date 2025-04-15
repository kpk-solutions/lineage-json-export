import pandas as pd

# Step 1: DataFrame setup
data = {
    'Group_level': [-4, -3, -1, 3, 4, 5],
    'Jobname': ['Pred03', 'Pred04', 'Abc01', 'Abc01', 'Sucr01', 'Sucr02'],
    'Relatedpredjobs': ['Pred02', 'Pred03', 'Pred04', 'Sucr01', 'Sucr02', 'Sucr03']
}
df = pd.DataFrame(data)

# Step 2: Helper maps to speed up lookup
negative_df = df[df['Group_level'] < 0]
positive_df = df[df['Group_level'] > 0]

# Step 3: Core recursive logic
def build_lineage(root):
    visited = {}
    
    def add_node(job, level):
        if job not in visited:
            visited[job] = {
                'id': job,
                'level': level,
                'asc': [],
                'desc': []
            }
        else:
            visited[job]['level'] = level  # Overwrite level for consistency

    # Step 4: Traverse Ascendants
    def get_ascendants(job, level):
        matches = negative_df[negative_df['Jobname'] == job]
        for _, row in matches.iterrows():
            asc = row['Relatedpredjobs']
            add_node(asc, level - 1)
            visited[job]['asc'].append(asc)
            visited[asc]['desc'].append(job)
            get_ascendants(asc, level - 1)

    # Step 5: Traverse Descendants
    def get_descendants(job, level):
        matches = positive_df[positive_df['Jobname'] == job]
        for _, row in matches.iterrows():
            desc = row['Relatedpredjobs']
            add_node(desc, level + 1)
            visited[job]['desc'].append(desc)
            visited[desc]['asc'].append(job)
            get_descendants(desc, level + 1)

    # Step 6: Start from root
    add_node(root, 0)
    get_ascendants(root, 0)
    get_descendants(root, 0)

    # Step 7: Final output
    nodes = list(visited.values())
    return {'root': root, 'nodes': sorted(nodes, key=lambda x: x['level'])}

# Run for root
root_job = 'Abc01'
output = build_lineage(root_job)

# Display result
from pprint import pprint
pprint(output)
