import json
import sys
from loguru import logger
from fastapi import FastAPI
from E2e_v9 import full_lineage
from fastapi.middleware.cors import CORSMiddleware


logger.remove()
logger.add(sys.stdout, level="DEBUG", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{message}</level>", colorize=True)


app = FastAPI()

# Allow requests from your frontend (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or use ["http://localhost:8000"] for safety
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/lineage/{job_name}")
async def get_lineage(job_name: str):
    logger.info('Calling lineage logic')
    lineage = full_lineage(job_name)
    logger.info(f'Lineage data: {lineage}')
    logger.info('Returning the response')
    return lineage

# import sql_connection
#
# @app.get("/sql/{job_name}")
# async def get_db(job_name: str):
#     row = sql_connection.connection()
#     result = {"row": str(row)}
#     return {job_name: result}
