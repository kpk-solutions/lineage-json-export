-- count check
SELECT COUNT(*)
FROM `project.dataset.Ctm_pred_impact_vw` t
WHERE NOT EXISTS (
  SELECT 1
  FROM `project.dataset.bq_master_vw` m
  WHERE t.db_name = m.db_name
    AND t.table_name = m.table_name
);

-- delete
DELETE FROM `project.dataset.Ctm_pred_impact_vw` t
WHERE NOT EXISTS (
  SELECT 1
  FROM `project.dataset.bq_master_vw` m
  WHERE t.db_name = m.db_name
    AND t.table_name = m.table_name
);

-- chunk by hash
from pyspark.sql.functions import hash, col

for i in range(0, 100):
    chunk_df = impact_df.filter(hash(col("db_name")) % 100 == i)

    cleaned = chunk_df.join(
        master_df,
        ["db_name", "table_name"],
        "left_anti"
    )

    cleaned.write.mode("append").saveAsTable("ctm_clean_temp")
