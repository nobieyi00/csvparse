# Databricks Notebook: Robust CSV Parsing with PySpark
# Created on 2025-05-24

# COMMAND ----------

# 1. Import required modules
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# 2. Define the schema explicitly
schema = StructType([
    StructField("Customer ID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("City, State", StringType(), True),
    StructField("Country", StringType(), True)
])

# COMMAND ----------

# 3. Preview the raw file (optional but useful)
raw_df = spark.read.text("/dbfs/tmp/sample_dirty_header.csv")
raw_df.show(truncate=False)

# COMMAND ----------

# 4. Read the CSV with proper options
df = spark.read.csv(
    "dbfs:/tmp/sample_dirty_header.csv",
    header=False,
    schema=schema,
    quote='"',
    escape='"',
    multiLine=False,
    mode="FAILFAST"
).option("skipRows", 1)

# COMMAND ----------

# 5. Show the parsed DataFrame
df.show(truncate=False)

# COMMAND ----------

# 6. Print the schema to confirm column structure
df.printSchema()

# COMMAND ----------

# 7. (Optional) Write to Delta table or Parquet for further processing
# df.write.format("delta").mode("overwrite").save("/mnt/processed/customer_data")
