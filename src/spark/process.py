from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, count
import re


class ProcessWikiData:
    def __init__(self, file):
        self.file = file
        self.spark = SparkSession.builder.getOrCreate()
        self.format = "xml"
        self.row_tag_revision = "revision"
        self.row_tag_title = 'page'
        self.text_tag = "text"
        self.revision_df = self.get_revision_df_from_xml()
        self.article_df = self.get_article_df_from_xml()
        self.df_link_count = None

    # parse xml and extract information under revision tag
    def get_revision_df_from_xml(self):
        revision_df = self.spark.read.format(self.format).options(rowTag=self.row_tag_revision).load(self.file)
        return revision_df

    # parse xml and extract information under revision tag
    def get_article_df_from_xml(self):
        article_df = self.spark.read.format(self.format).options(rowTag=self.row_tag_title).load(self.file)
        return article_df

    # extract text from the data frame of xml
    def extract_text(self):
        df_text = self.revision_df.select(self.text_tag)
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


# return list of link titles from a text if exist, else return empty list
def find_links(text):
    try:
        match_list = re.findall('\[\[[^\[\]]+\]\]', text[0])
        return match_list
    except:
        return []


if __name__ == "__main__":
    input_file = "s3a://wikipedia-article-sample-data/history1.xml-p10p933.bz2"
    process = ProcessWikiData(input_file)
    process.revision_df.show()
    process.article_df.show()
    #df_link = process.create_df_count_links()
