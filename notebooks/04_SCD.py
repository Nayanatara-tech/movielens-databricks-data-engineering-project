# Databricks notebook source
# MAGIC %md
# MAGIC ###simulate SCD
# MAGIC ###inserting a new record
# MAGIC ###updating an existing one
# MAGIC ###unchanged row will be there as it is

# COMMAND ----------

df_user_genre=spark.table("movies_schema.user_genre_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies_schema.user_genre_silver

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col
df_dup_records=(
    [
        Row(userId=10,favourite_genre="Action",count=100,rank=1), #update row
        Row(userId=21,favourite_genre="Drama",count=93,rank=1),#unchanged row
        Row(userId=140000,favourite_genre="Comedy",count=100,rank=1),#new row
    ]
)

df_update=spark.createDataFrame(df_dup_records)
df_update=df_update.withColumn("rank",col("rank").cast("int"))
df_update.createOrReplaceTempView("updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from updates

# COMMAND ----------

# MAGIC %md
# MAGIC ###Approach 1
# MAGIC ###Update - retiring old records - marking it as false to keep history - the genre changed row is updated as false and the non changed one is unaffected

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE movies_schema.user_genre_silver
# MAGIC SET current_status='False',
# MAGIC end_date=current_date()
# MAGIC where movies_schema.user_genre_silver.userId in (
# MAGIC   select g.userId from movies_schema.user_genre_silver g join updates u on u.userId=g.userId and g.current_status='True' and g.favourite_genre<>u.favourite_genre
# MAGIC ) 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies_schema.user_genre_silver where userId in (10,21)

# COMMAND ----------

# MAGIC %md
# MAGIC ###insert logic - to insert new records completely and since the updated ones are now marked as false ..the modified ones are added as new records now

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into movies_schema.user_genre_silver
# MAGIC select 
# MAGIC u.userId,
# MAGIC u.favourite_genre,
# MAGIC u.count,
# MAGIC u.rank,
# MAGIC current_date(),
# MAGIC Null,
# MAGIC 'True'
# MAGIC from updates u
# MAGIC left join movies_schema.user_genre_silver m on u.userId=m.userId and m.current_status='True' 
# MAGIC where m.userId is null
# MAGIC OR u.favourite_genre <> m.favourite_genre

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies_schema.user_genre_silver where userId in (10,21,140000)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Approach 2 using staged updates and merge operation

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col
df_dup_records_1=(
    [
        Row(userId=11,favourite_genre="Drama",count=229,rank=1), #update row
        Row(userId=22,favourite_genre="Action",count=83,rank=1),#unchanged row
        Row(userId=140001,favourite_genre="Sci-Fi",count=100,rank=1),#new row
    ]
)

df_update_1=spark.createDataFrame(df_dup_records_1)
df_update_1=df_update_1.withColumn("rank",col("rank").cast("int"))
df_update_1.createOrReplaceTempView("updates_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ###staged dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW staged_updates AS 
# MAGIC   select 
# MAGIC   u1.userId,
# MAGIC   u1.favourite_genre,
# MAGIC   u1.count,
# MAGIC   u1.rank,
# MAGIC   'Update' as Action
# MAGIC   from updates_1 u1
# MAGIC   join movies_schema.user_genre_silver m on 
# MAGIC   u1.userId=m.userId and m.current_status='True'
# MAGIC   and u1.favourite_genre<>m.favourite_genre
# MAGIC
# MAGIC   union all
# MAGIC
# MAGIC   select 
# MAGIC   u1.userId,
# MAGIC   u1.favourite_genre,
# MAGIC   u1.count,
# MAGIC   u1.rank,
# MAGIC   'Insert' as Action
# MAGIC   from updates_1 u1
# MAGIC left join movies_schema.user_genre_silver m on u1.userId=m.userId and m.current_status='True' 
# MAGIC where m.userId is null
# MAGIC OR u1.favourite_genre <> m.favourite_genre
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from staged_updates 

# COMMAND ----------

# MAGIC %md
# MAGIC ###merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO movies_schema.user_genre_silver as target
# MAGIC using staged_updates as source
# MAGIC on target.userId=source.userId
# MAGIC and target.current_status='True'
# MAGIC and source.Action='Update'
# MAGIC
# MAGIC when MATCHED and source.action='Update'
# MAGIC THEN
# MAGIC update set 
# MAGIC target.current_status='False',
# MAGIC target.end_date=current_date()
# MAGIC
# MAGIC when not matched and source.action='Insert'
# MAGIC THEN
# MAGIC INSERT (
# MAGIC     userId,
# MAGIC     favourite_genre,
# MAGIC     count,
# MAGIC     rank,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     current_status
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.userId,
# MAGIC     source.favourite_genre,
# MAGIC     source.count,
# MAGIC     source.rank,
# MAGIC     current_date(),
# MAGIC     NULL,
# MAGIC     'True'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies_schema.user_genre_silver where userId in (11,22,140001)