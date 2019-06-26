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
        # self.page_df_text = self.get_page_df_from_xml()  # data frame with text
        # self.page_df_links = self.create_df_of_links()   # data frame with links
        # self.page_df_id_link_time = self.explode_links()   # data frame with exploded links
        # self.df_earliest_timestamp = self.group_by_id_link()  # find the earliest timestamp for a link in an article

    # parse xml and extract information under revision tag
    def get_page_df_from_xml(self):
        # customSchema = StructType([StructField("id", IntegerType(), True),
        #                            StructField("revision",
        #                                        StructType([StructField("id", IntegerType(), True)]), True),
        #                            ])

        # customSchema = StructType([StructField("id", IntegerType(), True)])

        # .option("excludeAttribute", "false")
        # .option("rowTag", "elem")

        page_df = self.spark.read\
            .format(self.format) \
            .option("excludeAttribute", "false")\
            .options(rowTag=self.row_tag_page)\
            .load(self.file)

        page_df.printSchema()
        print(page_df.count(), len(page_df.columns))
        page_df.show()

        # page_df.selectExpr("explode(revision.id) as rev")\
        #     .select("rev").show(100)

        revision_df_id = page_df.select(f.col('id'), f.col('revision.id'))
        revision_df_id.show()

        revision_df = self.spark.read\
            .format(self.format) \
            .option("excludeAttribute", "false") \
            .options(rowTag=self.row_tag_revision)\
            .load(self.file)

        revision_df.printSchema()
        print(revision_df.count(), len(revision_df.columns))
        revision_df.show()


        # xmlDF.withColumn("xmlcomment", explode(
        #     sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "book").load($"xmlcomment")))

        # create df with article id, text, and revision timestamp
        # df_id_text_time = revision_df.select(f.col('parentid'),
        #                                      f.col('text'),
        #                                      f.col('timestamp'))
        #
        # # cast timestamp as timestamp type for future query
        # df_id_text_time = df_id_text_time.withColumn("time", df_id_text_time.timestamp.cast(TimestampType()))
        # df_id_text_time.show(n=100)

        return revision_df

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
        df_earliest_timestamp = self.page_df_id_link_time.groupby("id", "link").agg(f.min("time").alias("time"))
        df = df_earliest_timestamp.selectExpr("id as article_id", "link as link_name", "time as first_time_stamp")
        return df


# return list of link titles from a text if exist, else return empty list
def find_links(text):
    import re
    try:
        match_list = re.findall('\[\[[^\[\]]+\]\]', text[0])
        link_list = map(lambda x: x[2:-2], match_list)
        sub = "User:"
        link_list_no_user = [link for link in link_list if not sub in link]
        return list(link_list_no_user)
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

    df_link_count.select('article_id', 'link_name', 'first_time_stamp').\
        write.jdbc(url=jdbc_url,
                   table='links',
                   properties=connection_properties,
                   mode='append')

    print("POSTGRESQL DONE")


if __name__ == "__main__":
    large_data = "s3a://wiki-history/history1.xml-p10572p11357.bz2"   # 2gb
    medium_file = "s3a://wiki-history/history18.xml-p13693074p13784345.bz2"  # 800mb
    small_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"    #50mb
    small_rev_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799rev"
    current_file = "s3a://wiki-meta/meta-current_test1.xml.bz2"

    process = ParseXML(current_file)
    process.get_page_df_from_xml()
    # df_id_link_count = process.page_df_id_link_time.groupby("id", "link").count().sort(desc("count"))

    # print_df_count(process.page_df_id_link_time)
    # print_df_count(process.df_earliest_timestamp)

    # hostname = "ec2-34-239-95-229.compute-1.amazonaws.com"
    # database = "test"
    # port = "5432"
    # url = "jdbc:postgresql://{0}:{1}/{2}".format(hostname, port, database)
    # write_to_postgres(df_link_count=process.df_earliest_timestamp, jdbc_url=url)
    #
