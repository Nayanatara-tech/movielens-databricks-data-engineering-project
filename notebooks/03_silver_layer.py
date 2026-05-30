# Databricks notebook source
# MAGIC %md
# MAGIC ###Read from bronze layer

# COMMAND ----------

df_movies_bronze=spark.table("movies_schema.movies_bronze")
df_ratings_bronze=spark.table("movies_schema.ratings_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ###movies transformations to create Movie dimension table

# COMMAND ----------

# MAGIC %md
# MAGIC ###removing duplicates

# COMMAND ----------

df_movies_clean_dup=df_movies_bronze.dropDuplicates(["movieId"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###filtering non nulls in primary key column

# COMMAND ----------

df_movies_clean_not_null=df_movies_clean_dup.filter("movieId is not null")

# COMMAND ----------

# MAGIC %md
# MAGIC ###replacing title nulls to 'Unknown'

# COMMAND ----------

df_movies_clean_title_fillna=df_movies_clean_not_null.fillna({"title":"Unknown"})
df_movies_clean_title_fillna.filter("title!='Unknown'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###extracting movie year from the column title and cleaning title with only names

# COMMAND ----------

from pyspark.sql.functions import regexp_extract,regexp_replace,expr

df_movies_clean_extracting_year=df_movies_clean_title_fillna.withColumn("release_year",expr("try_cast(regexp_extract(title, '\\\\((\\\\d{4})\\\\)', 1) as int)"))\
    .withColumn("title",regexp_replace("title",r"\((\d{4})\)",""))

# COMMAND ----------

display(df_movies_clean_extracting_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ###saving silver_movies dimension table before exploding genres

# COMMAND ----------

df_movies_clean_extracting_year.write.format("delta").mode("overwrite").saveAsTable("movies_schema.movies_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ###saving silver_movies_genres dimension table 

# COMMAND ----------

from pyspark.sql.functions import split,explode
df_movies_silver=spark.table("movies_schema.movies_silver")
df_movies_explode_silver=df_movies_silver.withColumn("genres",explode(split("genres",r"\|")))
display(df_movies_explode_silver)


# COMMAND ----------

df_movies_explode_silver.write.format("delta").mode("overwrite").saveAsTable("movies_schema.movies_genres_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ###rating - watch events fact table

# COMMAND ----------

# MAGIC %md
# MAGIC ### removing duplicates 

# COMMAND ----------

df_ratings_clean=df_ratings_bronze.dropDuplicates(["userId","movieId","timestamp"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###removing nulls

# COMMAND ----------

df_ratings_clean=df_ratings_clean.filter("userId is not null and movieId is not null")


# COMMAND ----------

# MAGIC %md
# MAGIC ###filter invalid ratings

# COMMAND ----------

from pyspark.sql.functions import col
df_ratings_clean=df_ratings_clean.filter((col("rating")>=0.5) & (col("rating")<=5.0))

# COMMAND ----------

# MAGIC %md
# MAGIC ###watch events silver fact table

# COMMAND ----------

df_movies_explode_silver=df_movies_explode_silver.withColumnRenamed("ingestion_time","movies_ingestion_time")\
    .withColumnRenamed("source","movies_source")\
    .withColumnRenamed("batch_id","movies_batch_id")

# COMMAND ----------

df_watch_events=df_ratings_clean.join(df_movies_explode_silver,"movieId","inner")

# COMMAND ----------

display(df_watch_events)

# COMMAND ----------

# MAGIC %md
# MAGIC ###save fact_watch_events table

# COMMAND ----------

df_watch_events.write.mode("overwrite").format("delta").saveAsTable("movies_schema.watch_events_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ###create scd2 table with user profile - top genre per user

# COMMAND ----------

# MAGIC %md
# MAGIC ###find favourite genre

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
df_user_genre=df_watch_events.groupBy("userId","genres").count()

window=Window.partitionBy("userId").orderBy(col("count").desc())
df_user_genre=df_user_genre.withColumn("rank",row_number().over(window))
df_user_genre=df_user_genre.filter("rank=1")
display(df_user_genre)



# COMMAND ----------

# MAGIC %md
# MAGIC ###add scd columns

# COMMAND ----------

from pyspark.sql.functions import current_date,lit
df_user_genre=df_user_genre.withColumnRenamed("genres","favourite_genre")\
    .withColumn("start_date",current_date())\
    .withColumn("end_date",lit(None).cast("date"))\
    .withColumn("current_status",lit(True))

display(df_user_genre)

# COMMAND ----------

# MAGIC %md
# MAGIC ###save table

# COMMAND ----------

df_user_genre.write.mode("overwrite").format("delta").saveAsTable("movies_schema.user_genre_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC