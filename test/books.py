from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

input_file = "s3a://wikipedia-article-sample-data/books.xml"

df = spark.read.format('xml').options(rowTag='book').load(input_file)
df.printSchema()
df.show()

df_author_id = df.select("author", "_id")
df_author_id.printSchema()
df_author_id.show()
