import pyspark
from delta import *
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()


def test_validate_rating_data_lake():
    records_count = spark.read.format('delta').load('delta lake/ratings').count()
    assert records_count >= 1


def test_validate_movie_data_lake():
    records_count = spark.read.format('delta').load('delta lake/movies').count()
    assert records_count >=1


def test_rating_schema_inserted():
    ratings_schema = StructType([
      StructField("user_id", StringType(), True), \
      StructField("item_id", IntegerType(), True), \
      StructField("rating", FloatType(), True), \
      StructField("Timestamp", IntegerType(), True)])
    ratings = spark.read.format('csv').schema(ratings_schema).load('ml-100k/u.data', sep='\t').schema
    delta_rating = spark.read.format('delta').load('delta lake/ratings').schema
    assert ratings == delta_rating


def test_movies_schema_inserted():
    movies = spark.read.csv('ml-100k/u.item', sep='|'). \
      toDF("movie_id", "movie_title", "release_date",
           "video_release_date",
           "IMDb_URL", "unknown",
           "Action", "Adventure", "Animation",
           "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
           "Film_Noir", "Horror", "Musical", "Mystery", "Romance", "Sci_Fi",
           "Thriller", "War", "Western").schema

    delta_movies = spark.read.format('delta').load('delta lake/movies').schema
    assert movies == delta_movies


def test_ratings_duplicates_inserted():
    ratings_schema = StructType([
        StructField("user_id", StringType(), True), \
        StructField("item_id", IntegerType(), True), \
        StructField("rating", FloatType(), True), \
        StructField("Timestamp", IntegerType(), True)])
    ratings = spark.read.format('csv').schema(ratings_schema).load('ml-100k/u.data', sep='\t')
    assert ratings.groupby(['user_id', 'item_id']).count().where('count > 1').count() == 0



