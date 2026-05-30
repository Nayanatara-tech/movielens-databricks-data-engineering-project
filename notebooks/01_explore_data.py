# Databricks notebook source
# MAGIC %md
# MAGIC ###Checking sample data

# COMMAND ----------

df_movies=spark.table("movies_schema.movies")
df_ratings=spark.table("movies_schema.rating")

display(df_movies.limit(10))
display(df_ratings.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema exploration

# COMMAND ----------

df_movies.printSchema()
df_ratings.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###injecting nulls and duplicates to practice

# COMMAND ----------

dup_movies=df_movies.limit(5)
dup_ratings=df_ratings.limit(5)

df_movies_raw=df_movies.union(dup_movies)
df_ratings_raw=df_ratings.union(dup_ratings)

df_movies_raw.write.mode("overwrite").saveAsTable("movies_schema.movies_raw")
df_ratings_raw.write.mode("overwrite").saveAsTable("movies_schema.ratings_raw")


# COMMAND ----------

from pyspark.sql.functions import when,col
df_movies_null=df_movies_raw.withColumn("title",when(col("movieId")%10==0,None).otherwise(col("title")))
df_movies_null.write.mode("overwrite").saveAsTable("movies_schema.movies_raw")

# COMMAND ----------

df_movies=spark.table("movies_schema.movies_raw")
df_ratings=spark.table("movies_schema.ratings_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ###statistics check

# COMMAND ----------

df_ratings.describe().show()
df_movies.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Null check

# COMMAND ----------

from pyspark.sql.functions import col, sum

df_movies.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_movies.columns
]).show()

# COMMAND ----------

for c in df_movies.columns:
    df_movies.select(
        sum(col(c).isNull().cast("int")).alias(c)
    ).show()

# COMMAND ----------

df_movies.filter(col("movieId").isNull()).show()
df_movies.filter(col("title").isNull()).show()
df_movies.filter(col("genres").isNull()).show()

# COMMAND ----------

df_ratings.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_ratings.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Duplicate check

# COMMAND ----------

df_movies.groupBy("genres","movieId","title").count().filter(col("count")>1).show()

# COMMAND ----------

df_movies.groupBy("movieId","title").count().filter(col("count")>1).show()

# COMMAND ----------

df_ratings.groupBy("userId","movieId","timestamp").count().filter(col("count")>1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct in primary key check

# COMMAND ----------

df_movies.select("movieId").distinct().count()


# COMMAND ----------

display(df_movies.count())

# COMMAND ----------

df_ratings.select("userId").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###data range analysis

# COMMAND ----------

df_ratings.select("rating").distinct().orderBy("rating").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##time range analysis

# COMMAND ----------

df_ratings.selectExpr("min(timestamp)","max(timestamp)").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##data skew analysis

# COMMAND ----------

df_movies.groupBy("movieId").count().orderBy("count",ascending=False).show()

# COMMAND ----------

df_ratings.groupBy("movieId","userId").count().orderBy("count",ascending=False).show()