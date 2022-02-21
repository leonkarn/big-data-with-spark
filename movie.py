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
        # Ingest movies to Delta Lake
        self.df.write.format("delta").mode("append").save('delta lake/'+file_name)

    # Transformation
    def export_transposed_movie_genre(self, filename):
        pd_df = pd.DataFrame(columns=['movie id', 'title', 'category'])

        movie_categ = ["Action", "Adventure", "Animation",
                       "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
                       "Film_Noir", "Horror", "Musical", "Mystery", "Romance", "Sci_Fi",
                       "Thriller", "War", "Western"]

        movies_df = self.df.toPandas()
        # We loop the dataframe and check in every row for every movie where there is one and after we add it
        # in a new dataframe
        for i in range(len(movies_df)):
            movie = movies_df.loc[i, ["movie_id", "movie_title"]]
            all_cat = movies_df[movie_categ].iloc[i]
            for j in range(len(all_cat)):
                if all_cat[j] == str(1):
                    pd_df.loc[len(pd_df)] = [movie["movie_id"], movie["movie_title"], all_cat.index.values[j]]

        pd_df.to_csv('results/'+filename, index=False)

    # Implement a method that find the top 10 films by rating. Each of the top 10 films should have at least 5 ratings.
    # Order by the highest rated film first and write the results out to a single CSV file.
    def export_top_films(self, rating_df, file_name, top_films=10):
        self.df.createOrReplaceTempView("movies")
        rating_df.createOrReplaceTempView("ratings")

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
