import pyspark
from delta import *
from movie import Movie
from rating import Rating

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()


def test_validate_rating_data_lake():
    records_count = spark.read.format('delta').load('delta lake/ratings').count()
    assert records_count >= 1


def test_validate_movie_data_lake():
    records_count = spark.read.format('delta').load('delta lake/movies').count()
    assert records_count >= 1


def test_rating_schema_inserted():
    ratings_ingested = Rating(spark, 'ml-100k/u.data')
    delta_ratings = spark.read.format('delta').load('delta lake/ratings')
    assert ratings_ingested.schema == delta_ratings.schema


def test_movies_schema_inserted():
    ingested_movies = Movie(spark, 'ml-100k/u.item')
    delta_movies = spark.read.format('delta').load('delta lake/movies')
    assert ingested_movies.df.schema == delta_movies.schema


def test_ratings_duplicates_inserted():
    ratings_ingested = Rating(spark, 'ml-100k/u.data').df
    assert ratings_ingested.groupby(['user_id', 'item_id']).count().where('count > 1').count() == 0


def test_check_delta_lake_ratings_duplicates():
    spark.read.format('delta').load('delta lake/ratings').createOrReplaceTempView("ratings")
    count_duplicates = spark.sql("""
            select user_id,item_id, count(*)
            from ratings
            group by user_id, item_id
            having count(*) > 1
            """
           ).count()
    assert count_duplicates == 0




