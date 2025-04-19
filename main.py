import json
from fastapi import FastAPI
from E2e_v9 import full_lineage
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

# Allow requests from your frontend (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or use ["http://localhost:8000"] for safety
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/lineage/")
async def get_lineage(job_name: str):
    lineage = full_lineage(job_name)
    # print(lineage)
    # lineage = json.dumps(lineage)
    return lineage

# import sql_connection
#
# @app.get("/sql/{job_name}")
# async def get_db(job_name: str):
#     row = sql_connection.connection()
#     result = {"row": str(row)}
#     return {job_name: result}
