import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType


class Movie:
    def __init__(self, spark_conf, path, **kwargs):
        self.spark = spark_conf
        self.path = path
        self.schema = StructType([
            StructField("movie_id", IntegerType(), True),
            StructField("movie_title", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("video_release_date", DateType(), True),
            StructField("IMDb_URL", StringType(), True),
            StructField("unknown", StringType(), True),
            StructField("Action", StringType(), True),
            StructField("Adventure", StringType(), True),
            StructField("Animation", StringType(), True),
            StructField("Children", StringType(), True),
            StructField("Comedy", StringType(), True),
            StructField("Crime", StringType(), True),
            StructField("Documentary", StringType(), True),
            StructField("Drama", StringType(), True),
            StructField("Fantasy", StringType(), True),
            StructField("Film_Noir", StringType(), True),
            StructField("Horror", StringType(), True),
            StructField("Musical", StringType(), True),
            StructField("Mystery", StringType(), True),
            StructField("Romance", StringType(), True),
            StructField("Sci_Fi", StringType(), True),
            StructField("Thriller", StringType(), True),
            StructField("War", StringType(), True),
            StructField("Western", StringType(), True)
        ])
        self.df = self.spark.read.format('csv').option("mergeSchema", "true").schema(self.schema).load(self.path,
                                                                                                       sep='|')

    def ingest_to_delta(self, file_name):
        """
        Ingest movie data to delta lake
        :param file_name: name of the folder where the parquets are going to be saved in the delta lake
        """
        self.df.write.format("delta").mode("overwrite").save('delta lake/'+file_name)

    # Implement a method for splitting the movie genres so that there is a single genre per row
    def export_transposed_movie_genre(self, filename):
        """
        Creates a pandas dataframe and loops through it and for every movie it finds all the categories for
        every movie and after it ingests the category and the movie in every row in a new dataframe

        :param filename: name of the CSV file that the results are going to be saved under resutls folder
        """
        pd_df = pd.DataFrame(columns=['movie id', 'title', 'category'])

        movie_categ = ["Action", "Adventure", "Animation",
                       "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
                       "Film_Noir", "Horror", "Musical", "Mystery", "Romance", "Sci_Fi",
                       "Thriller", "War", "Western"]

        delta_movies = self.spark.read.format('delta').load('delta lake/movies').toPandas()

        # We loop the dataframe and check in every row for every movie where there is one and after we add it
        # in a new dataframe
        for i in range(len(delta_movies)):
            movie = delta_movies.loc[i, ["movie_id", "movie_title"]]
            all_cat = delta_movies[movie_categ].iloc[i]
            for j in range(len(all_cat)):
                if all_cat[j] == str(1):
                    pd_df.loc[len(pd_df)] = [movie["movie_id"], movie["movie_title"], all_cat.index.values[j]]

        pd_df.to_csv('results/'+filename, index=False)

    # Implement a method that find the top 10 films by rating. Each of the top 10 films should have at least 5 ratings.
    # Order by the highest rated film first and write the results out to a single CSV file.
    def export_top_films(self, file_name, top_films=10):
        """
        It finds top films by rating that have at least 5 rating ordered by rating
         and writes the results in a CSV file in results folder

        :param file_name: name of the file we want to export inside the results folder
        :param top_films: the number of top films by rating we want to export
        """

        self.spark.read.format('delta').load('delta lake/movies').createOrReplaceTempView("movies")
        self.spark.read.format('delta').load('delta lake/ratings').createOrReplaceTempView("ratings")

        sqldf = self.spark.sql("""
        select item_id,movies.movie_title,mean(rating) as avg_rating
        from ratings
        join movies on ratings.item_id=movies.movie_id
        group by  item_id,movies.movie_title
        having count(*)>5
        order by avg_rating desc
        limit {}
        """.format(top_films))
        sqldf.toPandas().to_csv('results/'+file_name, index=False)
