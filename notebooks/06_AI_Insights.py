# Databricks notebook source
# MAGIC %md
# MAGIC ###connecting to ai model to generate insights - with generic prompt

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC genres,
# MAGIC avg_rating,
# MAGIC     
# MAGIC     ai_query(
# MAGIC       'databricks-meta-llama-3-3-70b-instruct',
# MAGIC
# MAGIC       CONCAT(
# MAGIC         'Generate a short business insight for genre ',
# MAGIC         genres,
# MAGIC         ' with average rating ',
# MAGIC         avg_rating
# MAGIC       )
# MAGIC
# MAGIC     ) AS ai_insight
# MAGIC
# MAGIC FROM movies_schema.genre_rating_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ###refining prompts

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC genres,
# MAGIC avg_rating,
# MAGIC     
# MAGIC     ai_query(
# MAGIC       'databricks-meta-llama-3-3-70b-instruct',
# MAGIC
# MAGIC       CONCAT(
# MAGIC         'You are a movie business analyst.
# MAGIC         Generate:
# MAGIC         - 3 concise bullet insights
# MAGIC         - 1 business opportunity
# MAGIC         - 1 risk
# MAGIC         Keep response under 120 words. ',
# MAGIC         genres,
# MAGIC         ' with average rating ',
# MAGIC         avg_rating
# MAGIC       )
# MAGIC
# MAGIC     ) AS ai_insight
# MAGIC
# MAGIC FROM movies_schema.genre_rating_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table movies_schema.genre_ai_insights_gold as
# MAGIC select 
# MAGIC genres,
# MAGIC avg_rating,
# MAGIC     
# MAGIC     ai_query(
# MAGIC       'databricks-meta-llama-3-3-70b-instruct',
# MAGIC
# MAGIC       CONCAT(
# MAGIC         'You are a movie business analyst.
# MAGIC         Generate:
# MAGIC         - 3 concise bullet insights
# MAGIC         - 1 business opportunity
# MAGIC         - 1 risk
# MAGIC         Keep response under 120 words. ',
# MAGIC         genres,
# MAGIC         ' with average rating ',
# MAGIC         avg_rating
# MAGIC       )
# MAGIC
# MAGIC     ) AS ai_insight
# MAGIC
# MAGIC FROM movies_schema.genre_rating_gold
# MAGIC