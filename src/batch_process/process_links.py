from pyspark.sql import SparkSession
from pyspark.sql.functions import count
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
    .option("dbtable", "links") \
    .option("numPartitions", '10000') \
    .load()


#jdbcDF2 = spark.read.jdbc(url=url, table="links", properties=properties)
tx_df.show(20)
print(tx_df.printSchema())
# print(tx_df.count(), len(tx_df.columns))
# print("Postgres to Spark done")


df_link_count = tx_df.groupBy('link').agg(count('*').alias('count'))
df_link_count.show(20)
print(df_link_count.printSchema())
print(df_link_count.count())
print("POSTGRES TO SPARK DONE")


df_link_count.select('link', 'count').\
        write.jdbc(url=url,
                   table='pages_linked_count',
                   properties=properties,
                   mode='append')

print("POSTGRES DONE")
