from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import desc
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
        self.page_df_text = self.get_page_df_from_xml()  # data frame with text
        self.page_df_links = self.create_df_of_links()   # data frame with links
        self.page_df_id_link_time = self.explode_links()   # data frame with exploded links
        self.link_df_min_timestamp = self.group_by_id_link()  # find the earliest timestamp for a link in an article

    # parse xml and extract information under revision tag
    def get_page_df_from_xml(self):

        page_df = self.spark.read\
            .format(self.format)\
            .options(rowTag=self.row_tag_title)\
            .load(self.file)\
            .persist()

        # create df with article id, text, and revision timestamp
        df_id_text_time = page_df.select(f.col('id'),
                                         f.col('revision.text'),
                                         f.col('revision.timestamp'))

        # cast timestamp as timestamp type for future query
        df_id_text_time = df_id_text_time.withColumn("time", df_id_text_time.timestamp.cast(TimestampType()))

        return df_id_text_time

    # extract links from the text and create data frame with list of link titles
    def create_df_of_links(self):

        find_links_udf = udf(find_links, ArrayType(StringType()))

        # find links from the text column using regex with udf from df with text column
        df = self.page_df_text.withColumn('links',
                                          find_links_udf(self.page_df_text.text))

        # dataframe with article id, revision timestamp, array of links in the text
        df_links = df.select(f.col('id'),
                             f.col('time'),
                             f.col('links'))

        return df_links

    # create column with a single link
    def explode_links(self):
        # create column of single link name
        df_id_link_time = self.page_df_links.withColumn("link", explode(self.page_df_links.links))

        # create dataframe with article id, revision timestamp, link name (dropping links)
        page_df_id_link_time = df_id_link_time.select(f.col('id'),
                                                      f.col('time'),
                                                      f.col('link'))

        return page_df_id_link_time

    # when multiple revisions, find the earliest creation date for a link in an article
    def group_by_id_link(self):
        df_earliest_timestamp = self.page_df_id_link_time.groupby("id", "link").agg(f.min("time"))
        return df_earliest_timestamp


# return list of link titles from a text if exist, else return empty list
def find_links(text):
    try:
        match_list = re.findall('\[\[[^\[\]]+\]\]', text[0])
        link_list = map(lambda x: x[2:-2], match_list)
        return list(link_list)
    except:
        return []


# helper for printing dataframe number of rows and columns
def print_df_count(df):
    df.printSchema()
    print(df.count(), len(df.columns))
    df.show()


if __name__ == "__main__":
    input_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
    process = ParseXML(input_file)
    print_df_count(process.page_df_text)

    print_df_count(process.page_df_links)
    print_df_count(process.page_df_id_link_time)

    df_count = process.page_df_id_link_time.groupby("id", "link").count().sort(desc("count"))
    df_count.show()

    print_df_count(process.link_df_min_timestamp)
