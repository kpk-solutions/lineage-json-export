import json
import argparse
import pandas as pd
from collections import defaultdict, deque

from orca.eventsynthesizer import routeToObject

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
direction = 'downstream'
df = pd.DataFrame(data)
df["relatedpredjobs"] = df["relatedpredjobs"].apply(lambda x : [i.strip() for i in x.split(',')])
asc_graph = defaultdict(list)
dsc_graph = defaultdict(list)

for _,row in df.iterrows():
    job = row["jobname"]
    for related_job in row["relatedpredjobs"]:
        if row["grouped_level"]<0:
            asc_graph[job].append(related_job)
            dsc_graph[related_job].append(job)
        else:
            asc_graph[related_job].append(job)
            dsc_graph[job].append(related_job)
vistors = {}
root = df.loc[df["grouped_level"]==-1]["jobname"].values[0]
vistors[root] = {"id":root, "level":0, "asc":[],"dsc":[]}

if direction in ["upstream", 'e2e']:
    asc_queue = deque([(root, 0)])
    while asc_queue:
        key_job , level = asc_queue.popleft()
        for asc_job in asc_graph.get(key_job, []):
            if asc_job not in vistors:
                vistors[asc_job] = {"id":asc_job, "level":level-1, "asc":[],"dsc":[]}
                asc_queue.append((asc_job, level-1))
            if asc_job not in vistors[key_job]["asc"]:
                vistors[key_job]["asc"].append(asc_job)
            if key_job not in vistors[asc_job]["dsc"]:
                vistors[asc_job]["dsc"].append(key_job)

if direction in ["downstream", 'e2e']:
    dsc_queue = deque([(root, 0)])
    while dsc_queue:
        key_job , level = dsc_queue.popleft()
        for dsc_job in dsc_graph.get(key_job, []):
            if dsc_job not in vistors:
                vistors[dsc_job] = {"id":dsc_job, "level":level-1, "asc":[],"dsc":[]}
                dsc_queue.append((dsc_job, level-1))
            if dsc_job not in vistors[key_job]["dsc"]:
                vistors[key_job]["dsc"].append(dsc_job)
            if key_job not in vistors[dsc_job]["asc"]:
                vistors[dsc_job]["asc"].append(key_job)
result  = {"root":root,
          "nodes":sorted(vistors.values(), key=lambda x: x["level"])}
print(json.dumps(result, indent=2))