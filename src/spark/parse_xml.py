from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import desc
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, count


class ParseXML:
    def __init__(self, file):
        self.file = file
        self.spark = SparkSession.builder.getOrCreate()
        self.format = "xml"
        self.row_tag_revision = "revision"
        self.row_tag_page = 'page'
        self.row_tag_id = 'id'
        self.df_main_pages_text = self.get_page_text_column(print_table_info=True)
        self.page_df_text = self.get_page_text_column(print_table_info=True)  # data frame with text
        self.page_id_title = self.get_df_with_page_id_title(print_table_info=True)  # df only article title and ID
        self.page_id_links = self.get_df_article_id_links(print_table_info=True)  # page id and links in list
        #
        # self.page_df_links = self.create_df_of_links()   # data frame with links
        self.page_df_id_link_time = self.explode_links()   # data frame with exploded links

    # parse xml and extract information under page tag, filter only main articles
    def get_page_df_from_xml(self, print_table_info: bool):
        page_df = self.spark.read\
            .format(self.format) \
            .option("excludeAttribute", "false")\
            .options(rowTag=self.row_tag_page)\
            .load(self.file)
        print_df_count(page_df) if print_table_info else None

        # Filter only main articles by its namespace and pages that are not redirecting
        main_articles = page_df.filter((page_df.ns == 0) & (f.isnull('redirect')))
        print_df_count(main_articles) if print_table_info else None

        return main_articles

    # PAGE_ID: int, PAGE_TITLE: str, REVISION_ID: int, TIME_STAMP: timestamp, TEXT: list with 1 element
    def get_page_text_column(self, print_table_info: bool):
        df_main_pages = self.get_page_df_from_xml(print_table_info=True)
        df_articles_text = df_main_pages.select(f.col('id').alias('page_id'),
                                                f.col('title').alias('page_title'),
                                                f.col('revision.id').alias("revision_id"),
                                                f.col('revision.timestamp'),
                                                f.col('revision.text'))

        df_articles_text = df_articles_text.withColumn("time_stamp", df_articles_text.timestamp.cast(TimestampType()))
        print_df_count(df_articles_text) if print_table_info else None

        return df_articles_text

    # PAGE ID: int, PAGE TITLE: str
    def get_df_with_page_id_title(self, print_table_info: bool):
        df_article_id_title = self.df_main_pages_text.select(f.col('page_id'),
                                                             f.col('page_title'),
                                                             f.col("time_stamp")).distinct()
        print_df_count(df_article_id_title) if print_table_info else None

        return df_article_id_title

    # PAGE ID: int, LINKS: list
    def get_df_article_id_links(self, print_table_info: bool):
        find_links_udf = udf(find_links, ArrayType(StringType()))

        # find links from the text column using regex with udf from df with text column
        df = self.page_df_text.withColumn('links', find_links_udf(self.page_df_text.text))

        df_page_count_links = df.select(f.col('page_title'),
                                        f.col('time_stamp'),
                                        f.col('links'),
                                        f.size('links').alias('link_cnt'))
        print_df_count(df_page_count_links) if print_table_info else None

        return df_page_count_links

    # # PAGE_ID: int, PAGE_TITLE: str, REVISION_ID: int, TIME_STAMP: timestamp, LINKS: list with 1 element
    # def create_df_of_links(self):
    #
    #     find_links_udf = udf(find_links, ArrayType(StringType()))
    #
    #     # find links from the text column using regex with udf from df with text column
    #     df = self.page_df_text.withColumn('links',
    #                                       find_links_udf(self.page_df_text.text))
    #
    #     # dataframe with article id, revision timestamp, array of links in the text
    #     df_links = df.select(f.col('page_id'),
    #                          f.col('page_title'),
    #                          f.col('revision_id'),
    #                          f.col('time_stamp'),
    #                          f.col('links'))
    #
    #     return df_links

    # (each link is a row):  PAGE_ID: int, PAGE_TITLE: str, REVISION_ID: int, TIME_STAMP: timestamp, LINK: str
    def explode_links(self):
        # create column of single link name
        df_id_link_time = self.page_id_links.withColumn("link", explode(self.page_id_links.links))

        # create dataframe with article id, revision timestamp, link name (dropping links)
        page_df_id_link_time = df_id_link_time.select(f.col('page_title'),
                                                      f.col('link'))

        return page_df_id_link_time

    def count_num_each_link_in_page(self):
        df = self.page_df_id_link_time.groupby("page_title", "link").\
            agg(f.count(f.lit(1)).alias("link_count"))
        return df


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

        distinct_links = list(set(links_url_name))
        return distinct_links
    except:
        return []


# helper for printing dataframe number of rows and columns
def print_df_count(df):
    df.printSchema()
    print(df.count(), len(df.columns))
    df.show()


# write link and count data frame from spark to postgres
def write_to_postgres(df_link_count, jdbc_url):
    connection_properties = {
        "user": "postgres",
        "password": "$password",
        "driver": "org.postgresql.Driver"
    }

    df_link_count.select('page_id', 'page_title', 'revision_id', 'link', 'time_stamp', 'link_count').\
        write.jdbc(url=jdbc_url,
                   table='pages_links',
                   properties=connection_properties,
                   mode='append')

    print("POSTGRESQL DONE")


if __name__ == "__main__":
    large_data = "s3a://wiki-history/history1.xml-p10572p11357.bz2"   # 2gb
    medium_file = "s3a://wiki-history/history18.xml-p13693074p13784345.bz2"  # 800mb
    small_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"    #50mb
    small_rev_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799rev"
    current_file = "s3a://wiki-meta/meta-current1.xml.bz2"  #200mb
    current_large_file = "s3a://wiki-meta/meta-current27.xml.bz2"  #628mb
    current_file_2 = "s3a://wiki-current-part2/current2.xml-p30304p88444.bz2"  # 200mb

    current_part_1 = "s3a://wiki-current-part1/*"

    process = ParseXML(small_file)
    # process.get_page_df_from_xml()
    # df_id_link_count = process.page_df_id_link_time.groupby("id", "link").count().sort(desc("count"))

    # print_df_count(process.page_df_text)
    # print_df_count(process.page_df_links)
    # print_df_count(process.page_df_id_link_time)

    df_count_links = process.count_num_each_link_in_page()
    print_df_count(df_count_links)

    # hostname = "ec2-34-239-95-229.compute-1.amazonaws.com"
    # database = "wikimain"
    # port = "5432"
    # url = "jdbc:postgresql://{0}:{1}/{2}".format(hostname, port, database)
    # write_to_postgres(df_link_count=df_count_links, jdbc_url=url)



