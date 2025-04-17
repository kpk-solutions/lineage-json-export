import pandas as pd

def build_lineage(df: pd.DataFrame, root: str):
    lineage = {}
    
    def add_node(job_id, level, asc_list=None, desc_list=None):
        if job_id not in lineage:
            lineage[job_id] = {"id": job_id, "level": level, "asc": [], "desc": []}
        if asc_list:
            lineage[job_id]["asc"].extend(x for x in asc_list if x not in lineage[job_id]["asc"])
        if desc_list:
            lineage[job_id]["desc"].extend(x for x in desc_list if x not in lineage[job_id]["desc"])

    # Start with root
    add_node(root, 0)

    # Upstream
    asc_queue = [(root, 0)]
    while asc_queue:
        current_id, current_level = asc_queue.pop(0)
        asc_rows = df[(df["Jobname"] == current_id) & (df["Group_level"] < 0)]
        for _, row in asc_rows.iterrows():
            new_id = row["Relatedpredjobs"]
            new_level = row["Group_level"]
            add_node(new_id, new_level, asc_list=[], desc_list=[current_id])
            lineage[current_id]["asc"].append(new_id)
            asc_queue.append((new_id, new_level))

    # Downstream
    desc_queue = [(root, 0)]
    while desc_queue:
        current_id, current_level = desc_queue.pop(0)
        desc_rows = df[(df["Jobname"] == current_id) & (df["Group_level"] > 0)]
        for _, row in desc_rows.iterrows():
            new_id = row["Relatedpredjobs"]
            new_level = row["Group_level"]
            add_node(new_id, new_level, asc_list=[current_id], desc_list=[])
            lineage[current_id]["desc"].append(new_id)
            desc_queue.append((new_id, new_level))

    return {"root": root, "nodes": list(lineage.values())}

# Sample dataframe
df = pd.DataFrame({
    'Group_level': [-4, -3, -1, 3, 4, 5],
    'Jobname': ['Pred03', 'Pred04', 'Abc01', 'Abc01', 'Sucr01', 'Sucr02'],
    'Relatedpredjobs': ['Pred02', 'Pred03', 'Pred04', 'Sucr01', 'Sucr02', 'Sucr03']
})

result = build_lineage(df, root="Abc01")
print(result)
