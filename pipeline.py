import pyspark
import os
import wget
import zipfile
import argparse

from delta import *
from rating import Rating
from movie import Movie


def download_data(url, zip_file):
    # download from movielens
    wget.download(url)
    zip_data = zipfile.ZipFile(zip_file)
    zip_data.extractall()
    # delete zip file
    os.remove(zip_file)

def create_folders():
    if not os.path.isdir('delta lake'):
        os.mkdir('delta lake')

    if not os.path.isdir('results'):
        os.mkdir('results')

def main(url, zip_file):
    # Download our data
    if not os.path.isdir('ml-100k'):
        download_data(url, zip_file)

    # Create delta lake and results if they don t exist
    create_folders()

    # Start SparkSessions
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # STAGING
    # rating
    newrating = Rating(spark, 'ml-100k/u.data')
    newrating.insert_or_update_to_delta('ratings')

    # movie
    newmovie = Movie(spark, 'ml-100k/u.item')
    newmovie.ingest_to_delta('movies')

    # TRANSFORMATION
    newmovie.export_transposed_movie_genre('movie_genre_transposed.csv')
    newmovie.export_top_films(newrating.df, 'top_movies.csv')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')
    parser.add_argument('-u', '--url', required=True)
    parser.add_argument('-z', '--zip_file', required=True)
    args = parser.parse_args()
    main(url=args.url, zip_file=args.zip_file)


