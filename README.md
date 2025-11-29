1️⃣ Read Data (Bronze Layer)

df = spark.read.format("csv") \
 .option("header", "true") \
 .load("/mnt/raw/data.csv")

2️⃣ Clean + Transform (Silver)

df_clean = df.filter(df["status"] == "ACTIVE") \
 .withColumn("amount", df["amount"].cast("double"))

3️⃣ Write to Delta (Gold)
df_clean.write.format("delta") \
 .mode("overwrite") \
 .save("/mnt/gold/clean_data")

4️⃣ Create a Delta Table

CREATE TABLE sales_gold
USING DELTA
LOCATION '/mnt/gold/clean_data';

5️⃣ Optimize for Query Performance

OPTIMIZE sales_gold ZORDER BY (customer_id);

6️⃣ Auto Loader for Incremental Pipelines

df_inc = (spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .load("/mnt/raw/incoming/"))

7️⃣ Merge (Upsert) into Delta

MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

