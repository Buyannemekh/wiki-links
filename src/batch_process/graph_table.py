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


tx_df = spark.read\
    .format("jdbc") \
    .option("url", url) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", "pages") \
    .load()

tx_df.show(20)
sorted_table = tx_df.orderBy("page_id", ascending=False)
sorted_table.show(20)

tx_df_distinct = sorted_table.dropDuplicates(['page_id'])


look_up_df = tx_df_distinct.select(col('page_id').alias('link_id'),
                           col('page_title'))
# look_up_df.show(20)
# print(look_up_df.printSchema())
# print(look_up_df.count(), len(look_up_df.columns))

pages_links_df = tx_df_distinct.withColumn("link", explode(tx_df.links)).select(col('page_id'), col('link'))
# print(pages_links_df.printSchema())
# print(pages_links_df.count(), len(pages_links_df.columns))
# pages_links_df.show(20)


end_table = look_up_df.join(pages_links_df, look_up_df.page_title == pages_links_df.link).select('link_id', 'page_title', 'page_id')
# print(end_table.printSchema())
# print(end_table.count(), len(end_table.columns))
# end_table.show(20)

#orderby_link_id_df = end_table.orderBy("link_id")
#print(orderby_link_id_df.printSchema())
#print(orderby_link_id_df.count(), len(orderby_link_id_df.columns))
#orderby_link_id_df.show(20)

popularity_df = end_table.groupBy('link_id').agg(count('*').alias('cite_count'))
# print(popularity_df.printSchema())
# print(popularity_df.count(), len(popularity_df.columns))
# popularity_df.show(20)


pages_in_out = tx_df_distinct.join(popularity_df, tx_df_distinct.page_id == popularity_df.link_id).select('page_id',
                                                                                                          'page_title',
                                                                                                          'time_stamp',
                                                                                                          'links',
                                                                                                          'link_cnt',
                                                                                                          'cite_count')
# print(pages_in_out.printSchema())
# print(pages_in_out.count(), len(pages_in_out.columns))
# pages_in_out.show(20)


pages_in_out.select('page_id', 'page_title', 'time_stamp', 'links', 'link_cnt', 'cite_count').\
    write.jdbc(url=url,
               table='pages_in_out',
               properties=properties,
               mode='append')

print("POSTGRES DONE")
