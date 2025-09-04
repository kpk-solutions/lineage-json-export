from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
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
SOURCE_FILE_PATTERN = re.compile(r"[^\s'\"]+", re.IGNORECASE)  # Match any file
TARGET_FILE_PATTERN = re.compile(r">?\s*([^\s'\"]+)", re.IGNORECASE)  # Match any file
PARAM_PATTERN = re.compile(r"\$[a-zA-Z0-9_]+")

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
    source_tables = [f"{db}.{tbl}" for db, tbl in source_tables] if source_tables else ["NA"]
    target_tables = [f"{db}.{tbl}" for db, tbl in target_tables] if target_tables else ["NA"]
    source_files = list(set(source_files)) if source_files else ["NA"]
    target_files = list(set(target_files)) if target_files else ["NA"]
    parameters = list(set(parameters)) if parameters else ["NA"]

    # Return rows (exploded style)
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
# Collect Data From Folder (All Files with Logging)
# --------------------------
def process_scripts(root_folder, log_file="processed_files.log"):
    all_rows = []

    with open(log_file, "w") as log:
        for dirpath, _, filenames in os.walk(root_folder):
            for file in filenames:
                script_path = os.path.join(dirpath, file)
                if os.path.isfile(script_path):
                    log.write(f"{script_path}\n")  # Log every detected file
                    try:
                        all_rows.extend(extract_metadata(script_path))
                    except Exception as e:
                        log.write(f"Error processing file {script_path}: {e}\n")

    df = spark.createDataFrame(all_rows, schema=schema)
    df = df.dropDuplicates()
    return df

# --------------------------
# Run Example
# --------------------------
if __name__ == "__main__":
    input_folder = "/path/to/your/scripts"  # 🔹 Change this to your folder
    final_df = process_scripts(input_folder)

    final_df.show(truncate=False)
    final_df.write.mode("overwrite").parquet("output/script_metadata")
