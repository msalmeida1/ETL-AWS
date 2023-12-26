import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

dynamic_frame_movies = glueContext.create_dynamic_frame.from_catalog(
    database="my_database",
    table_name="movies"
)
dynamic_frame_series = glueContext.create_dynamic_frame.from_catalog(
    database="my_database",
    table_name="series"
)

dynamic_frame_genre_movie = dynamic_frame_movies.select_fields(["genre_id", "genre_name"]).toDF()
dynamic_frame_actors_movie = dynamic_frame_movies.select_fields(["actor_id", "actor_name", "actor_gender", "actor_popularity"]).toDF()
dynamic_frame_companies_movie = dynamic_frame_movies.select_fields(["production_companies_id", "production_companies_name"]).toDF()
dynamic_frame_sub_movie_genres = dynamic_frame_movies.select_fields(["id", "genre_id"]).toDF()
dynamic_frame_sub_movie_actors = dynamic_frame_movies.select_fields(["id", "actor_id"]).toDF()
dynamic_frame_sub_movie_company = dynamic_frame_movies.select_fields(["id", "production_companies_id"]).toDF()
dynamic_frame_movies_data = dynamic_frame_movies.select_fields(["id", "title", "genre_id", "release_date", "budget", "revenue", "runtime", "vote_average", "vote_count", "popularity", "status", "actor_id", "origin_country", "production_companies_id"]).toDF()
dynamic_frame_genre_serie = dynamic_frame_series.select_fields(["genre_id", "genre_name"]).toDF()
dynamic_frame_actors_serie = dynamic_frame_series.select_fields(["actor_id", "actor_name", "actor_gender", "actor_popularity"]).toDF()
dynamic_frame_companies_serie = dynamic_frame_series.select_fields(["production_companies_id", "production_companies_name"]).toDF()
dynamic_frame_sub_serie_genres = dynamic_frame_series.select_fields(["id", "genre_id"]).toDF()
dynamic_frame_sub_serie_actors = dynamic_frame_series.select_fields(["id", "actor_id"]).toDF()
dynamic_frame_sub_serie_company = dynamic_frame_series.select_fields(["id", "production_companies_id"]).toDF()
dynamic_frame_series_data = dynamic_frame_series.select_fields(["id", "name", "status", "in_production", "first_air_date", "last_air_date", "number_of_episodes", "number_of_seasons", "popularity", "vote_count", "vote_average", "origin_country", "genre_id", "actor_id", "production_companies_id"]).toDF()

dynamic_frame_genre_movie.createOrReplaceTempView("genre_movie")
dynamic_frame_actors_movie.createOrReplaceTempView("actors_movie")
dynamic_frame_companies_movie.createOrReplaceTempView("companies_movie")
dynamic_frame_sub_movie_genres.createOrReplaceTempView("sub_movie_genres")
dynamic_frame_sub_movie_actors.createOrReplaceTempView("sub_movie_actors")
dynamic_frame_sub_movie_company.createOrReplaceTempView("sub_movie_company")
dynamic_frame_movies_data.createOrReplaceTempView("movies_data")
dynamic_frame_genre_serie.createOrReplaceTempView("genre_serie")
dynamic_frame_actors_serie.createOrReplaceTempView("actors_serie")
dynamic_frame_companies_serie.createOrReplaceTempView("companies_serie")
dynamic_frame_sub_serie_genres.createOrReplaceTempView("sub_serie_genres")
dynamic_frame_sub_serie_actors.createOrReplaceTempView("sub_serie_actors")
dynamic_frame_sub_serie_company.createOrReplaceTempView("sub_serie_company")
dynamic_frame_series_data.createOrReplaceTempView("series_data")

result_genre_movie = spark.sql("SELECT * FROM genre_movie")
result_actors_movie = spark.sql("SELECT * FROM actors_movie")
result_companies_movie = spark.sql("SELECT * FROM companies_movie")
result_sub_movie_genres = spark.sql("SELECT * FROM sub_movie_genres")
result_sub_movie_actors = spark.sql("SELECT * FROM sub_movie_actors")
result_sub_movie_company = spark.sql("SELECT * FROM sub_movie_company")
result_movies_data = spark.sql("SELECT * FROM movies_data")
result_genre_serie = spark.sql("SELECT * FROM genre_serie")
result_actors_serie = spark.sql("SELECT * FROM actors_serie")
result_companies_serie = spark.sql("SELECT * FROM companies_serie")
result_sub_serie_genres = spark.sql("SELECT * FROM sub_serie_genres")
result_sub_serie_actors = spark.sql("SELECT * FROM sub_serie_actors")
result_sub_serie_company = spark.sql("SELECT * FROM sub_serie_company")
result_series_data = spark.sql("SELECT * FROM series_data")

result_genre_movie.show()
result_actors_movie.show()
result_companies_movie.show()
result_sub_movie_genres.show()
result_sub_movie_actors.show()
result_sub_movie_company.show()
result_movies_data.show()
result_genre_serie.show()
result_actors_serie.show()
result_companies_serie.show()
result_sub_serie_genres.show()
result_sub_serie_actors.show()
result_sub_serie_company.show()
result_series_data.show()

output_path_movies = "s3://desafio-parte-1/Refined/Movies/2023/10/29/" 
output_path_series = "s3://desafio-parte-1/Refined/Series/2023/10/29/"

result_movies_data.write.mode("overwrite").parquet(output_path_movies + "result_movies_data")
result_genre_movie.write.mode("overwrite").parquet(output_path_movies + "result_genre_movie")
result_actors_movie.write.mode("overwrite").parquet(output_path_movies + "result_actors_movie")
result_companies_movie.write.mode("overwrite").parquet(output_path_movies + "result_companies_movie")
result_sub_movie_genres.write.mode("overwrite").parquet(output_path_movies + "result_sub_movie_genres")
result_sub_movie_actors.write.mode("overwrite").parquet(output_path_movies + "result_sub_movie_actors")
result_sub_movie_company.write.mode("overwrite").parquet(output_path_movies + "result_sub_movie_company")

result_series_data.write.mode("overwrite").parquet(output_path_series + "result_series_data")
result_genre_serie.write.mode("overwrite").parquet(output_path_series + "result_genre_serie")
result_actors_serie.write.mode("overwrite").parquet(output_path_series + "result_actors_serie")
result_companies_serie.write.mode("overwrite").parquet(output_path_series + "result_companies_serie")
result_sub_serie_genres.write.mode("overwrite").parquet(output_path_series + "result_sub_serie_genres")
result_sub_serie_actors.write.mode("overwrite").parquet(output_path_series + "result_sub_serie_actors")
result_sub_serie_company.write.mode("overwrite").parquet(output_path_series + "result_sub_serie_company")

spark.stop()