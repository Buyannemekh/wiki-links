from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import col, explode
import sys
import os


spark = SparkSession.builder.getOrCreate()

os.environ["POSTGRES_HOSTNAME"] = sys.argv[1]
os.environ["POSTGRES_USER"] = sys.argv[2]
os.environ["POSTGRES_PASSWORD"] = sys.argv[3]
os.environ["POSTGRES_DBNAME"] = sys.argv[4]

properties = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver"
}

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
hostname = os.environ["POSTGRES_HOSTNAME"]
database = os.environ["POSTGRES_DBNAME"]
port = "5432"
url = "jdbc:postgresql://{0}:{1}/{2}".format(hostname, port, database)


def read_postgres():
    df = spark.read\
        .format("jdbc") \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", "pages") \
        .load()

    main_page_df = df.orderBy("page_id", ascending=False).dropDuplicates(['page_id'])
    return main_page_df


def create_df_in_out_degree(main_page_df):
    look_up_df = main_page_df.select(col('page_id').alias('link_id'), col('page_title'))
    pages_links_df = main_page_df.withColumn("link", explode(main_page_df.links)).select(col('page_id'), col('link'))
    end_table = look_up_df.join(pages_links_df, look_up_df.page_title == pages_links_df.link).select('link_id',
                                                                                                     'page_title',
                                                                                                     'page_id')
    popularity_df = end_table.groupBy('link_id').agg(count('*').alias('cite_count'))
    pages_in_out = main_page_df.join(popularity_df, main_page_df.page_id == popularity_df.link_id).select('page_id',
                                                                                                          'page_title',
                                                                                                          'time_stamp',
                                                                                                          'links',
                                                                                                          'link_cnt',
                                                                                                          'cite_count')
    return pages_in_out


def main():
    main_page_df = read_postgres()
    pages_in_out_df = create_df_in_out_degree(main_page_df)
    # print(pages_in_out_df.printSchema())
    # print(pages_in_out_df.count(), len(pages_in_out_df.columns))
    pages_in_out_df.show(20)
    return pages_in_out_df


def write_to_postgres(pages_in_out):
    pages_in_out.select('page_id', 'page_title', 'time_stamp', 'links', 'link_cnt', 'cite_count').\
        write.jdbc(url=url,
                   table='pages_in_out',
                   properties=properties,
                   mode='append')

    print("POSTGRES DONE")


if __name__ == "__main__":
    pages_in_out_degree = main()
    # write_to_postgres(pages_in_out_degree)

