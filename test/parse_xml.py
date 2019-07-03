from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, count
import re
import os
import sys


class Pipeline:
    def __init__(self, file):
        self.file = file
        self.spark = SparkSession.builder.getOrCreate()
        self.format = "xml"
        self.row_tag = "revision"
        self.text_tag = "text"
        self.spark_df = self.get_df_from_xml()
        self.df_link_count = self.create_df_count_links()

    # parse xml and extract information under revision tag
    def get_df_from_xml(self):
        self.spark_df = self.spark.read.format(self.format) \
            .options(rowTag=self.row_tag) \
            .load(self.file)
        return self.spark_df

    # extract text from the data frame of xml
    def extract_text(self):
        df_text = self.spark_df.select(self.text_tag)
        return df_text

    # extract links from the text and create data frame with list of link titles
    def create_df_of_links(self):
        df_text = self.extract_text()
        find_links_udf = udf(find_links, ArrayType(StringType()))
        df_links = df_text.withColumn('links', find_links_udf(df_text.text))
        # df_links.show()
        df_links.where(df_links.links.isNotNull()).show()
        return df_links

    # explode the list of links and create df with unique link title and its count
    def create_df_count_links(self):
        df_links = self.create_df_of_links()
        df_links.select(explode(col('links')).alias('link')).show()
        self.df_link_count = df_links.select(explode(col('links')).alias('link')).\
            groupBy('link').agg(count('*').alias('link_count'))
        self.df_link_count.show()
        return self.df_link_count


# # return list of link titles from a text if exist, else return empty list
# def find_links(text):
#     try:
#         match_list = re.findall('\[\[[^\[\]]+\]\]', text[0])
#         return match_list
#     except:
#         return []

#
# # write link and count data frame from batch_process to postgres
# def write_to_postgres(df_link_count, jdbc_url):
#     connection_properties = {
#         "user": "postgres",
#         "password": "$password",
#         "driver": "org.postgresql.Driver"
#     }
#
#     df_link_count.select('link', 'link_count').\
#         write.jdbc(url=jdbc_url,
#                    table='wiki_links',
#                    properties=connection_properties,
#                    mode='overwrite')
#
#     print("POSTGRESQL DONE")


if __name__ == "__main__":
    input_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
    pipeline = Pipeline(input_file)

    # hostname = "ec2-34-239-95-229.compute-1.amazonaws.com"
    # database = "wiki"
    # port = "5432"
    # url = "jdbc:postgresql://{0}:{1}/{2}".format(hostname, port, database)
    # write_to_postgres(df_link_count=pipeline.df_link_count, jdbc_url=url)
