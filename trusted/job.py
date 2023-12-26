import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import to_date
from pyspark.sql.functions import explode
from awsglue.dynamicframe import DynamicFrame
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

caminho_s3 = "*****************"

df = glueContext.create_dynamic_frame_from_options(connection_type= 's3',connection_options={"paths": [caminho_s3]},format='json', format_options={
        "multiline": True
})

df = df.toDF()

df = df.withColumn("revenue", when(col("revenue.int").isNotNull(), col("revenue.int")).otherwise(col("revenue.long").cast("int")))
df_detalhes_expandido = df.withColumn("genre", explode(col("genres"))) \
    .select(
        col("id"),
        col("imdb_id"),
        col("title"),
        col("genre.id").alias("genre_id"),
        col("genre.name").alias("genre_name"),
        col("release_date"),
        col("budget"),
        col("revenue"),
        col("runtime"),
        col("vote_average"),
        col("vote_count"),
        col("popularity"),
        col("status"),
        col("data_de_processamento")
    )

df_production_companies_expandido = df.withColumn("production_company", explode(col("production_companies"))) \
    .select(
        col("id"),
        col("production_company.id").alias("production_companies_id"),
        col("production_company.name").alias("production_companies_name"),
    )


df_country_expandido = df.withColumn("country", explode(col("production_countries"))) \
    .select(
        col("id"),
        col("country.name").alias("origin_country")
    )

df_cast_expandido = df.withColumn("actor", explode(col("cast"))) \
    .select(
        col("id"),
        col("actor.id").alias("actor_id"),
        col("actor.name").alias("actor_name"),
        col("actor.gender").alias("actor_gender"),
        col("actor.popularity").alias("actor_popularity")
    )

df_join = df_detalhes_expandido.join(df_cast_expandido, "id")
df_join = df_join.join(df_country_expandido, "id")
df_join = df_join.join(df_production_companies_expandido, "id")

dynamic_frame_filmes = DynamicFrame.fromDF(df_join, glueContext, "json_filmes")

s3output = glueContext.getSink(
  path="**************",
  connection_type="s3",
  partitionKeys=[data_de_processamento],
  compression="snappy",
)

s3output.setFormat("glueparquet")
s3output.writeFrame(dynamic_frame_filmes)

## SERIES ##

caminho_s3 = "****************"

df = glueContext.create_dynamic_frame_from_options(connection_type= 's3',connection_options={"paths": [caminho_s3]},format='json', format_options={
        "multiline": True
})

df = df.toDF()

df_detalhes_expandido = df.withColumn("genre", explode(col("genres"))) \
    .select(
        col("id"),
        col("name"),
        col("status"),
        col("in_production"),
        col("first_air_date"),
        col("last_air_date"),
        col("number_of_episodes"),
        col("number_of_seasons"),
        col("popularity"),
        col("vote_count"),
        col("vote_average"),
        col("genre.id").alias("genre_id"),
        col("genre.name").alias("genre_name"),
        col("data_de_processamento")
    )

df_created_by_expanded = df.withColumn("creator", explode(col("created_by"))) \
    .select(
        col("id"),
        col("creator.name").alias("creator")
    )

df_production_companies_expandido = df.withColumn("production_company", explode(col("production_companies"))) \
    .select(
        col("id"),
        col("production_company.id").alias("production_companies_id"),
        col("production_company.name").alias("production_companies_name"),
    )

df_country_expandido = df.withColumn("country", explode(col("origin_country"))) \
    .select(
        col("id"),
        col("country").alias("origin_country")
    )

df_cast_expandido = df.withColumn("actor", explode(col("cast"))) \
    .select(
        col("id"),
        col("actor.id").alias("actor_id"),
        col("actor.name").alias("actor_name"),
        col("actor.gender").alias("actor_gender"),
        col("actor.popularity").alias("actor_popularity")
        
    )

df_join = df_detalhes_expandido.join(df_cast_expandido, "id")
df_join = df_join.join(df_country_expandido, "id")
df_join = df_join.join(df_created_by_expanded, "id")
df_join = df_join.join(df_production_companies_expandido, "id")

dynamic_frame_series = DynamicFrame.fromDF(df_join, glueContext, "json_filmes")

s3output = glueContext.getSink(
  path="*************",
  connection_type="s3",
  partitionKeys=[data_de_processamento],
  compression="snappy",
)

s3output.setFormat("glueparquet")
s3output.writeFrame(dynamic_frame_series)