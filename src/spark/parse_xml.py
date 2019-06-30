from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, ArrayType, StringType
from pyspark.sql.functions import col, size, explode, isnull, udf
import os
import sys


class ParseXML:
    def __init__(self, file):
        self.file = file
        self.spark = SparkSession.builder.getOrCreate()
        self.format = "xml"
        self.row_tag_revision = "revision"
        self.row_tag_page = 'page'
        self.row_tag_id = 'id'
        self.df_main_pages = self.get_page_df_from_xml(print_table_info=False)
        self.page_id_title = self.get_df_with_page_id_title(print_table_info=False)  # df only article title and ID
        self.page_id_links = self.get_df_article_id_links(print_table_info=True)  # page id and links in list
        self.page_df_id_link_time = self.explode_links(print_table_info=True)   # data frame with exploded links

    # parse xml and extract information under page tag, filter only main articles
    def get_page_df_from_xml(self, print_table_info: bool):
        page_df_raw = self.spark.read.format(self.format)\
                                     .option("excludeAttribute", "false")\
                                     .options(rowTag=self.row_tag_page)\
                                     .load(self.file)

        print_df_count(page_df_raw) if print_table_info else None

        # Filter only main articles by its namespace and pages that are not redirecting
        df_main_pages = page_df_raw.filter((page_df_raw.ns == 0) & (isnull('redirect')))
        print_df_count(df_main_pages) if print_table_info else None

        return df_main_pages

    # PAGE_ID: int, PAGE_TITLE: str, REVISION_ID: int, TIME_STAMP: timestamp, TEXT: list with 1 element
    def get_page_text_column(self, print_table_info: bool):
        df_pages_text = self.df_main_pages.select(col('id').alias('page_id'),
                                                  col('title').alias('page_title'),
                                                  col('revision.id').alias("revision_id"),
                                                  col('revision.timestamp'),
                                                  col('revision.text'))

        df_pages_text = df_pages_text.withColumn("time_stamp",
                                                 df_pages_text.timestamp.cast(TimestampType()))

        print_df_count(df_pages_text) if print_table_info else None

        return df_pages_text

    # PAGE ID: int, PAGE TITLE: str, TIMESTAMP
    def get_df_with_page_id_title(self, print_table_info: bool):
        df_pages_text = self.get_page_text_column(print_table_info=print_table_info)
        df_article_id_title = df_pages_text.select(col('page_id'),
                                                   col('page_title'),
                                                   col("time_stamp")).distinct()

        print_df_count(df_article_id_title) if print_table_info else None

        return df_article_id_title

    # PAGE ID: int, PAGE TITLE: str, TIMESTAMP, LINKS: list, LINK_COUNT: int
    def get_df_article_id_links(self, print_table_info: bool):
        find_links_udf = udf(find_links, ArrayType(StringType()))

        df_pages_text = self.get_page_text_column(print_table_info=print_table_info)
        df = df_pages_text.withColumn('links', find_links_udf(df_pages_text))

        df_page_count_links = df.select(col('page_id'),
                                        col('page_title'),
                                        col('time_stamp'),
                                        col('links'),
                                        size('links').alias('link_cnt'))
        print_df_count(df_page_count_links) if print_table_info else None

        return df_page_count_links

    # (each link is a row):  PAGE_ID: int, PAGE_TITLE: str, REVISION_ID: int, TIME_STAMP: timestamp, LINK: str
    def explode_links(self, print_table_info: bool):
        page_id_links = self.get_df_article_id_links(print_table_info=print_table_info)

        # create column of single link name
        df_id_link_time = self.page_id_links.withColumn("link", explode(self.page_id_links.links))

        # create dataframe with article id, revision timestamp, link name (dropping links)
        page_df_id_link_time = df_id_link_time.select(col('page_id'),
                                                      col('page_title'),
                                                      col('link'))
        print_df_count(page_df_id_link_time) if print_table_info else None

        return page_df_id_link_time


# return list of link titles from a text if exist, else return empty list
def find_links(text):
    # sub_list = [":"]
    import re
    try:
        match_list = re.findall('\[\[[^\[\]]+\]\]', text[0])
        link_names = map(lambda x: x[2:-2], match_list)

        sub = ":"
        valid_links = [link for link in link_names if not sub in link]

        sep = "|"
        links_url_name = [link.split(sep, 1)[0] if sep in link else link for link in valid_links]

        distinct_links = list(set(links_url_name))   # if link appeared multiple times in the same article, count as 1
        return distinct_links
    except:
        return []


# helper for printing dataframe number of rows and columns
def print_df_count(df):
    df.printSchema()
    print(df.count(), len(df.columns))
    df.show()


# write link and count data frame from spark to postgres
def write_pages_to_postgres(df_pages, jdbc_url, connection_properties):
    df_pages.select('page_id', 'page_title', 'time_stamp', 'links', 'link_cnt').\
        write.jdbc(url=jdbc_url,
                   table='pages',
                   properties=connection_properties,
                   mode='append')

    print("PAGES DONE")


# write link and count data frame from spark to postgres
def write_links_to_postgres(df_links, jdbc_url, connection_properties):
    df_links.select('page_id', 'page_title', 'link').\
        write.jdbc(url=jdbc_url,
                   table='links',
                   properties=connection_properties,
                   mode='append')

    print("LINKS DONE")
    

if __name__ == "__main__":
    small_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"    #50mb
    current_large_file = "s3a://wiki-meta/meta-current27.xml.bz2"  #628mb
    current_file_2 = "s3a://wiki-current-part2/current2.xml-p30304p88444.bz2"  # 200mb
    current_part_1 = "s3a://wiki-current-part1/*"
    current_part_2 = "s3a://wiki-current-part2/*"
    current_part_3 = "s3a://wiki-current-part3/*"

    os.environ["POSTGRES_HOSTNAME"] = sys.argv[1]
    os.environ["POSTGRES_USER"] = sys.argv[2]
    os.environ["POSTGRES_PASSWORD"] = sys.argv[3]
    os.environ["POSTGRES_DBNAME"] = sys.argv[4]

    process = ParseXML(small_file)

    properties = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver"
    }

    hostname = os.environ["POSTGRES_HOSTNAME"]
    database = os.environ["POSTGRES_DBNAME"]
    port = "5432"
    url = "jdbc:postgresql://{0}:{1}/{2}".format(hostname, port, database)

    write_pages_to_postgres(df_pages=process.page_id_links, jdbc_url=url, connection_properties=properties)
    write_links_to_postgres(df_links=process.page_df_id_link_time, jdbc_url=url, connection_properties=properties)
