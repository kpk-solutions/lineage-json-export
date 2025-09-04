from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F
import re
import os
from datetime import datetime

# --------------------------
# Initialize Spark
# --------------------------
spark = SparkSession.builder.appName("ScriptMetadataExtractor").getOrCreate()

# --------------------------
# Regex Patterns
# --------------------------
SOURCE_TABLE_PATTERN = re.compile(r"\b(?:from|join|with)\s+([a-zA-Z0-9_\$]+)\.([a-zA-Z0-9_\$]+)", re.IGNORECASE)
TARGET_TABLE_PATTERN = re.compile(r"\b(?:insert\s+into|update|delete\s+from|truncate\s+table)\s+([a-zA-Z0-9_\$]+)\.([a-zA-Z0-9_\$]+)", re.IGNORECASE)
SOURCE_FILE_PATTERN = re.compile(r"[^\s'\"]+\.(?:dat|csv)", re.IGNORECASE)
TARGET_FILE_PATTERN = re.compile(r">\s*([^\s'\"]+\.(?:dat|csv))", re.IGNORECASE)
PARAM_PATTERN = re.compile(r"\$[a-zA-Z0-9_]+")   # Parameters like $DB, $TABLE, $FILE_PATH

# --------------------------
# File Reader & Extractor
# --------------------------
def extract_metadata(script_path):
    try:
        with open(script_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception:
        return []

    # Extract values
    source_tables = SOURCE_TABLE_PATTERN.findall(content)
    target_tables = TARGET_TABLE_PATTERN.findall(content)
    source_files = SOURCE_FILE_PATTERN.findall(content)
    target_files = TARGET_FILE_PATTERN.findall(content)
    parameters = PARAM_PATTERN.findall(content)

    # Normalize values
    source_tables = [f"{db}.{tbl}" for db, tbl in source_tables] if source_tables else []
    target_tables = [f"{db}.{tbl}" for db, tbl in target_tables] if target_tables else []
    source_files = list(set(source_files)) if source_files else []
    target_files = list(set(target_files)) if target_files else []
    parameters = list(set(parameters)) if parameters else []

    # If nothing found → store NA
    if not source_tables:
        source_tables = ["NA"]
    if not target_tables:
        target_tables = ["NA"]
    if not source_files:
        source_files = ["NA"]
    if not target_files:
        target_files = ["NA"]
    if not parameters:
        parameters = ["NA"]

    # Return rows (exploded style → one row per item)
    rows = []
    for s_table in source_tables:
        db, tbl = (s_table.split(".", 1) + ["NA"])[:2] if "." in s_table else ("NA", s_table)
        for t_table in target_tables:
            t_db, t_tbl = (t_table.split(".", 1) + ["NA"])[:2] if "." in t_table else ("NA", t_table)
            for s_file in source_files:
                for t_file in target_files:
                    rows.append((
                        os.path.basename(script_path),
                        db,
                        tbl,
                        t_db,
                        t_tbl,
                        s_file,
                        t_file,
                        datetime.now(),
                        ",".join(parameters)
                    ))
    return rows

# --------------------------
# Schema Definition
# --------------------------
schema = StructType([
    StructField("script_file_name", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("target_database", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("target_file", StringType(), True),
    StructField("insert_date", TimestampType(), True),
    StructField("parameters_used", StringType(), True)
])

# --------------------------
# Collect Data From Folder
# --------------------------
def process_scripts(root_folder):
    all_rows = []
    for dirpath, _, filenames in os.walk(root_folder):
        for file in filenames:
            if file.endswith((".py", ".sh", ".ksh", ".bteq")):
                all_rows.extend(extract_metadata(os.path.join(dirpath, file)))

    df = spark.createDataFrame(all_rows, schema=schema)

    # Deduplicate to ensure unique rows
    df = df.dropDuplicates()

    return df

# --------------------------
# Run Example
# --------------------------
if __name__ == "__main__":
    input_folder = "/path/to/your/scripts"  # 🔹 CHANGE this to your GCS/local folder path
    final_df = process_scripts(input_folder)

    final_df.show(truncate=False)
    final_df.write.mode("overwrite").parquet("output/script_metadata")  # 🔹 Example save
