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
df["relatedpredjobs"] = df["relatedpredjobs"].apply(lambda x : [i.strip() for i in x.split(',')])
asc_graph = defaultdict(list)
desc_graph = defaultdict(list)
for _, row in df.iterrows():
    job = row["jobname"]
    for related in row["relatedpredjobs"]:
        if row["grouped_level"] <0:
            asc_graph[job].append(related)
            desc_graph[related].append(job)
        else:
            asc_graph[related].append(job)
            desc_graph[job].append(related)
        # print(f"asc_graph: {asc_graph}")
        # print(f"desc_graph: {desc_graph}")
visitors = {}
root = df.loc[df["grouped_level"] == -1]["jobname"].values[0]
visitors[root] = {"id":root, "level":0, "ascendants":[], "descendants":[]}
asc_queue = deque([(root, 0)])
# print(f"visitors: {visitors}")
# print(f"asc_queue: {asc_queue}")
# print(f"asc_queue left: {asc_queue.popleft()}")
# print(f"asc_graph: {asc_graph}")
while asc_queue:
    key_job, level = asc_queue.popleft() #(Abc01,0)
    # print(f"asc_queue: {asc_queue}")
    # print(f"current: {key_job} # level: {level}")
    # print(f"asc_graph: {asc_graph.get(key_job, [])}")
    for ascendant in asc_graph.get(key_job, []): #'Abc01': ['Pred04', 'Pred15', 'Pred16']
        if ascendant not in visitors: #{'Abc01': {'id': 'Abc01', 'level': 0, 'ascendants': [], 'descendants': []}}
            visitors[ascendant] = {"id":ascendant, "level":level-1, "ascendants":[], "descendants":[]}
            asc_queue.append((ascendant, level-1))
        if ascendant not in visitors[key_job]["ascendants"]:
            visitors[key_job]["ascendants"].append(ascendant)
        if key_job not in visitors[ascendant]["descendants"]:
            visitors[ascendant]["descendants"].append(key_job)
desc_queue = deque([(root, 0)])
while desc_queue:
    key_job, level = desc_queue.popleft() #(Abc01,0)
    # print(f"asc_queue: {desc_queue}")
    # print(f"current: {key_job} # level: {level}")
    # print(f"asc_graph: {desc_graph.get(key_job, [])}")
    for descendant in desc_graph.get(key_job, []): #'Abc01': ['Pred04', 'Pred15', 'Pred16']
        if descendant not in visitors: #{'Abc01': {'id': 'Abc01', 'level': 0, 'ascendants': [], 'descendants': []}}
            visitors[descendant] = {"id":descendant, "level":level+1, "ascendants":[], "descendants":[]}
            asc_queue.append((descendant, level+1))
        if descendant not in visitors[key_job]["descendants"]:
            visitors[key_job]["descendants"].append(descendant)
        if key_job not in visitors[descendant]["ascendants"]:
            visitors[descendant]["ascendants"].append(key_job)
result = {
        'root': root,
        'nodes': sorted(visitors.values(), key=lambda x: x['level'])
    }
print(json.dumps(result, indent=2))




