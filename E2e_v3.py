import pandas as pd

def build_lineage(df):
    # Preprocessing for fast lookups
    job_to_level = dict(zip(df['Jobname'], df['Group_level']))
    pred_map = {}
    
    for _, row in df.iterrows():
        pred_map.setdefault(row['Jobname'], set()).add(row['Relatedpredjobs'])
        pred_map.setdefault(row['Relatedpredjobs'], set())  # ensure all keys exist

    # Find root node: one whose level is 0 and whose asc has level -1
    root = None
    for _, row in df.iterrows():
        if row['Group_level'] == 0 and job_to_level.get(list(pred_map.get(row['Jobname'], []))[0], None) == -1:
            root = row['Jobname']
            break
    if not root:
        raise ValueError("Root not found")

    visited = {}
    nodes = []

    def dfs_up(job, level):
        if job in visited:
            return
        asc = list(pred_map.get(job, []))
        asc = [a for a in asc if job_to_level.get(a, 0) < 0]
        node = {
            "id": job,
            "level": level,
            "asc": asc,
            "desc": []
        }
        visited[job] = node
        nodes.append(node)
        for a in asc:
            dfs_up(a, level - 1)

    def dfs_down(job, level):
        if job not in visited:
            visited[job] = {
                "id": job,
                "level": level,
                "asc": [],
                "desc": []
            }
            nodes.append(visited[job])
        desc = [child for child, preds in pred_map.items() if job in preds and job_to_level.get(child, 0) > 0]
        visited[job]["desc"] = desc
        for d in desc:
            dfs_down(d, level + 1)

    # Start from root
    visited[root] = {"id": root, "level": 0, "asc": [], "desc": []}
    nodes.append(visited[root])

    # Upstream (asc)
    dfs_up(root, 0)

    # Downstream (desc)
    dfs_down(root, 0)

    return {
        "root": root,
        "nodes": sorted(nodes, key=lambda x: x["level"])
    }
# Sample Data
data = [
    (-4, "Pred03", "Pred02"),
    (-3, "Pred04", "Pred03"),
    (-1, "Abc01", "Pred04"),
    (3, "Abc01", "Sucr01"),
    (4, "Sucr01", "Sucr02"),
    (5, "Sucr02", "Sucr03"),
]

df = pd.DataFrame(data, columns=["Group_level", "Jobname", "Relatedpredjobs"])

result = build_lineage(df)

import json
print(json.dumps(result, indent=2))
