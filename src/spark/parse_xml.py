from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, count
import re


class ParseXML:
    def __init__(self, file):
        self.file = file
        self.spark = SparkSession.builder.getOrCreate()
        self.format = "xml"
        self.row_tag_revision = "revision"
        self.row_tag_title = 'page'
        self.page_df = self.get_page_df_from_xml()

    # parse xml and extract information under revision tag
    def get_page_df_from_xml(self):
        page_df = self.spark.read.format(self.format).options(rowTag=self.row_tag_title).load(self.file)
        df = page_df.select(f.col('id'), f.col('revision.text'), f.col('revision.timestamp'))
        df = df.withColumn("time", df.timestamp.cast(TimestampType()).drop("timestamp").withColumnRenamed("time", "timestamp"))
        return df


if __name__ == "__main__":
    input_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
    process = ParseXML(input_file)
    process.page_df.printSchema()
    process.page_df.show()

