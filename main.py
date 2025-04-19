import json
from fastapi import FastAPI
from E2e_v9 import full_lineage

app = FastAPI()

@app.get("/lineage/{job_name}")
async def get_lineage(job_name: str):
    lineage = full_lineage(job_name)
    # print(lineage)
    return lineage

# import sql_connection
#
# @app.get("/sql/{job_name}")
# async def get_db(job_name: str):
#     row = sql_connection.connection()
#     result = {"row": str(row)}
#     return {job_name: result}
