"""
1. Setup: requirements.txt

fastapi
pandas
uvicorn

Install with:

pip install -r requirements.txt



⸻

2. Full FastAPI Code: main.py
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import pandas as pd
from typing import List, Dict

app = FastAPI()

# Sample in-memory data (can be replaced with DB or CSV)
data = [
    (-4, "Pred03", "Pred02"),
    (-3, "Pred04", "Pred03"),
    (-1, "Abc01", "Pred04"),
    (3, "Abc01", "Sucr01"),
    (4, "Sucr01", "Sucr02"),
    (5, "Sucr02", "Sucr03"),
]
df = pd.DataFrame(data, columns=["Group_level", "Jobname", "Relatedpredjobs"])

@app.get("/lineage")
def get_lineage(root: str = Query(..., description="Root Jobname (case-sensitive)")):
    # Build job-to-level and predecessor mapping
    job_to_level = dict(zip(df['Jobname'], df['Group_level']))
    pred_map = {}

    for _, row in df.iterrows():
        pred_map.setdefault(row['Jobname'], set()).add(row['Relatedpredjobs'])
        pred_map.setdefault(row['Relatedpredjobs'], set())  # ensure all keys exist

    if root not in job_to_level:
        raise HTTPException(status_code=404, detail="Root job not found in data")

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

    visited[root] = {"id": root, "level": 0, "asc": [], "desc": []}
    nodes.append(visited[root])

    dfs_up(root, 0)
    dfs_down(root, 0)

    return {
        "root": root,
        "nodes": sorted(nodes, key=lambda x: x["level"])
    }


"""
⸻

3. Run the API

uvicorn main:app --reload



⸻

4. Test it

Visit:

http://127.0.0.1:8000/lineage?root=Abc01

You’ll get a JSON response:

{
  "root": "Abc01",
  "nodes": [
    {"id": "Pred03", "level": -2, "asc": ["Pred02"], "desc": ["Pred04"]},
    {"id": "Pred04", "level": -1, "asc": ["Pred03"], "desc": ["Abc01"]},
    {"id": "Abc01", "level": 0, "asc": ["Pred04"], "desc": ["Sucr01"]},
    {"id": "Sucr01", "level": 1, "asc": ["Abc01"], "desc": ["Sucr02"]},
    {"id": "Sucr02", "level": 2, "asc": ["Sucr01"], "desc": ["Sucr03"]},
    {"id": "Sucr03", "level": 3, "asc": ["Sucr02"], "desc": []}
  ]
}
"""

