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
        self.revision_df = self.get_revision_df_from_xml()
        self.article_df = self.get_page_df_from_xml()
        # self.df_link_count = None

    # parse xml and extract information under revision tag
    def get_revision_df_from_xml(self):
        revision_df = self.spark.read.format(self.format).options(rowTag=self.row_tag_revision).load(self.file)
        # convert time string to timestamp
        df = revision_df.withColumn("time", revision_df.timestamp.cast(TimestampType()))
        df.printSchema()
        return df

    # parse xml and extract information under revision tag
    def get_page_df_from_xml(self):

        article_df = self.spark.read.format(self.format).options(rowTag=self.row_tag_title).load(self.file)
        article_df.printSchema()
        article_df.select(f.col('revision.*')).show(truncate=False)
        return article_df


if __name__ == "__main__":
    input_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
    process = ParseXML(input_file)
    process.revision_df.show()
    # process.article_df.show()
    #df_link = process.create_df_count_links()
