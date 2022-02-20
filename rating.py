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

    def insert_or_update_to_delta(self, file_name):
        if DeltaTable.isDeltaTable(self.spark, file_name):
            deltaTable = DeltaTable.forPath(self.spark, file_name)
            deltaTable.alias("old_ratings").merge(
                self.df.alias("new_ratings"),
                "old_ratings.user_id = new_ratings.user_id and old_ratings.item_id = new_ratings.item_id"). \
                whenMatchedUpdate(set={"rating": "new_ratings.rating"}) \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            self.df.write.format("delta").mode("append").partitionBy('user_id').save('delta lake/'+file_name)

