from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

#input_file = "s3a://wikipedia-article-sample-data/books.xml"
input_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"

df = spark.read.format('xml').options(rowTag='page').load(input_file)
df.printSchema()
df.show()
print(df.count(), len(df.columns))

df.write.parquet("article.parquet")
