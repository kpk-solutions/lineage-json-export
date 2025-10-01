from pyspark.sql import functions as F

# STEP 1: Join df_main with df_details on script_file_name
df_joined = df_main.join(df_details, on="script_file_name", how="left")

# STEP 2: Join with df_prm on prm_file_name and taskflow_nm
df_temp = df_joined.join(df_prm, on=["prm_file_name", "taskflow_nm"], how="left")

# STEP 3: Pivot df_prm from key-value pairs to columns
df_pivoted = df_temp.groupBy(
    "script_file_name", "source_database", "source_table",
    "target_database", "target_table"
).pivot("key").agg(F.first("values"))

# STEP 4: Replace parameterized fields
final_df = df_pivoted.withColumn(
    "source_database",
    F.when(F.col("source_database").startswith("$"), F.col("source_database")).otherwise(F.col("source_database"))
).withColumn(
    "source_database",
    F.coalesce(F.col("source_database"), F.col("source_database"))
).withColumn(
    "source_table",
    F.when(F.col("source_table").startswith("$"), F.col("source_table")).otherwise(F.col("source_table"))
).withColumn(
    "target_database",
    F.when(F.col("target_database").startswith("$"), F.col("target_database")).otherwise(F.col("target_database"))
).withColumn(
    "target_table",
    F.when(F.col("target_table").startswith("$"), F.col("target_table")).otherwise(F.col("target_table"))
)

final_df.show(truncate=False)
