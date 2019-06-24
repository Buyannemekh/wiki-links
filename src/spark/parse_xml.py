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
        df = df.withColumn("time", df.timestamp.cast(TimestampType()))
        return df

    # extract links from the text and create data frame with list of link titles
    def create_df_of_links(self):
        #df_text = self.page_df.select('text')
        find_links_udf = udf(find_links, ArrayType(StringType()))
        df_links = self.page_df.withColumn('links', find_links_udf(self.page_df.text))
        df_links.printSchema()
        df_links.where(df_links.links.isNotNull()).show()
        return df_links


# return list of link titles from a text if exist, else return empty list
def find_links(text):
    try:
        match_list = re.findall('\[\[[^\[\]]+\]\]', text[0])
        return match_list
    except:
        return []

if __name__ == "__main__":
    input_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
    process = ParseXML(input_file)
    process.page_df.printSchema()
    process.page_df.show()
    process.create_df_of_links()

