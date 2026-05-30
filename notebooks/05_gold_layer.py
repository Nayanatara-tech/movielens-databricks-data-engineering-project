# Databricks notebook source
# MAGIC %md
# MAGIC ### popular genres among users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC favourite_genre,
# MAGIC count(*) as total_users
# MAGIC from movies_schema.user_genre_silver
# MAGIC WHERE current_status = 'True'
# MAGIC group by favourite_genre
# MAGIC ORDER BY total_users desc

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table movies_schema.top_genre_gold as
# MAGIC SELECT 
# MAGIC favourite_genre,
# MAGIC count(*) as total_users
# MAGIC from movies_schema.user_genre_silver
# MAGIC WHERE current_status = 'True'
# MAGIC group by favourite_genre
# MAGIC ORDER BY total_users desc

# COMMAND ----------

# MAGIC %md
# MAGIC ###Genre Trends Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC genres,
# MAGIC release_year,
# MAGIC count(*) as genre_count
# MAGIC from movies_schema.movies_genres_silver
# MAGIC where release_year is not null
# MAGIC group by genres,release_year
# MAGIC ORDER BY genre_count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table movies_schema.genre_year_gold as
# MAGIC select
# MAGIC genres,
# MAGIC release_year,
# MAGIC count(*) as genre_count
# MAGIC from movies_schema.movies_genres_silver
# MAGIC where release_year is not null
# MAGIC group by genres,release_year
# MAGIC ORDER BY genre_count desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### average genre ratings over the years

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies_schema.watch_events_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC genres,
# MAGIC round(avg(rating),2) as avg_rating
# MAGIC from movies_schema.watch_events_silver
# MAGIC group by genres
# MAGIC ORDER BY avg_rating desc
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table movies_schema.genre_rating_gold as 
# MAGIC select
# MAGIC genres,
# MAGIC round(avg(rating),2) as avg_rating
# MAGIC from movies_schema.watch_events_silver
# MAGIC group by genres
# MAGIC ORDER BY avg_rating desc