# from src.spark.parse_xml import *
# from pyspark.sql.types import *
# from pyspark.sql import functions as f
from src.spark.parse_xml import *


class CreateDataFrames:
    def __init__(self, main_rdd, print_table_info):
        self.main_rdd = main_rdd
        self.print_table_info = print_table_info

    # PAGE_ID: int, PAGE_TITLE: str, REVISION_ID: int, TIME_STAMP: timestamp, TEXT: list with 1 element
    def get_page_text_column(self, print_table_info: bool):
        df_pages_text = self.main_rdd.select(col('id').alias('page_id'),
                                             col('title').alias('page_title'),
                                             col('revision.id').alias("revision_id"),
                                             col('revision.timestamp'),
                                             col('revision.text'))

        df_pages_text = df_pages_text.withColumn("time_stamp",
                                                 df_pages_text.timestamp.cast(TimestampType()))

        print_df_count(df_pages_text) if print_table_info else None

        return df_pages_text


# helper for printing dataframe number of rows and columns
def print_df_count(df):
    df.printSchema()
    print(df.count(), len(df.columns))
    df.show()


if __name__ == "__main__":
    small_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"  # 50mb
    process = ParseXML(file=small_file)
    create_tables = CreateDataFrames(main_rdd=process.df_main_pages, print_table_info=True)
