# Databricks notebook source
# MAGIC %md
# MAGIC ###read raw tables

# COMMAND ----------

df_movies=spark.table("movies_schema.movies_raw")
df_ratings=spark.table("movies_schema.ratings_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ###add metadata columns

# COMMAND ----------

from pyspark.sql.functions import lit,current_timestamp
df_movies_bronze=df_movies.withColumn("ingestion_time",current_timestamp())\
  .withColumn("source",lit("movie_lens"))\
  .withColumn("batch_id",lit(1))

display(df_movies_bronze)

# COMMAND ----------

from pyspark.sql.functions import col,to_date
df_ratings_bronze=df_ratings.withColumn("ingestion_time",current_timestamp())\
  .withColumn("source",lit("movie_lens"))\
  .withColumn("batch_id",lit(1))\
  .withColumn("watch_date",to_date((col("timestamp"))))

display(df_ratings_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ###partition check

# COMMAND ----------

df_ratings_bronze.groupBy("watch_date").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###write as delta tables

# COMMAND ----------

df_movies_bronze.write.mode("overwrite").format("delta").saveAsTable("movies_schema.movies_bronze")
df_ratings_bronze.write.mode("overwrite").format("delta").partitionBy("watch_date").saveAsTable("movies_schema.ratings_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ###validate

# COMMAND ----------

df_movies_bronze=spark.table("movies_schema.movies_bronze")
df_ratings_bronze=spark.table("movies_schema.ratings_bronze")
display(df_movies_bronze)
display(df_ratings_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Check if it is partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL movies_schema.ratings_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS movies_schema.ratings_bronze