from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from delta import *

class Rating:
    def __init__(self, spark_conf, path, frmt='csv'):
        self.frmt = frmt
        self.spark = spark_conf
        self.path = path
        self.schema = StructType([
            StructField("user_id", StringType(), True), \
            StructField("item_id", IntegerType(), True), \
            StructField("rating", FloatType(), True), \
            StructField("Timestamp", IntegerType(), True)])
        self.df = self.spark.read.format(self.frmt).schema(self.schema).load(self.path, sep='\t')

    # update a row if it exists, else the row should be inserted
    def insert_or_update_to_delta(self, file_name):
        """
        Ingest the data to delta lake.If there is a match in user id and movie id in an existing delta lake
        it updates with the new rating.

        :param file_name: name of the folder where the parquets are going to be saved in the delta lake
        """
        if DeltaTable.isDeltaTable(self.spark, 'delta lake/'+file_name):
            deltaTable = DeltaTable.forPath(self.spark, 'delta lake/'+file_name)
            deltaTable.alias("old_ratings").merge(
                self.df.alias("new_ratings"),
                "old_ratings.user_id = new_ratings.user_id and old_ratings.item_id = new_ratings.item_id"). \
                whenMatchedUpdate(set={"rating": "new_ratings.rating"}) \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            self.df.write.format("delta").partitionBy('user_id').save('delta lake/'+file_name)

