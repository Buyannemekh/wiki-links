from pyspark.sql import SparkSession
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
# display_df(tx_df)

#jdbcDF2 = spark.read.jdbc(url=url, table="links", properties=properties)
tx_df.show()
print(tx_df.count(), len(tx_df.columns))
print("Postgres to Spark done")
