from src.spark.parse_xml import *
from pyspark.sql.types import *
from pyspark.sql import functions as f


class CreateTables:
    def __init__(self, main_rdd, print_table_info):
        self.main_rdd = main_rdd
        self.print_table_info = print_table_info

    # PAGE_ID: int, PAGE_TITLE: str, REVISION_ID: int, TIME_STAMP: timestamp, TEXT: list with 1 element
    def get_page_text_column(self):
        df_articles_text = self.main_rdd.select(f.col('id').alias('page_id'),
                                                f.col('title').alias('page_title'),
                                                f.col('revision.id').alias("revision_id"),
                                                f.col('revision.timestamp'),
                                                f.col('revision.text'))

        df_articles_text = df_articles_text.withColumn("time_stamp", df_articles_text.timestamp.cast(TimestampType()))
        if self.print_table_info:
            print_df_count(df_articles_text)

        return df_articles_text

    # PAGE ID: int, PAGE TITLE: str
    def get_df_with_page_id_title(self):
        df_article_id_title = self.main_rdd.select(f.col('id').alias('page_id'),
                                                   f.col('title').alias('page_title')).distinct()

        print_df_count(df_article_id_title) if self.print_table_info else None
        return df_article_id_title


if __name__ == "__main__":
    small_file = "s3a://wikipedia-article-sample-data/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"  # 50mb
    process = ParseXML(file=small_file, print_table_info=True)
    create_tables = CreateTables(main_rdd=process.df_main_pages, print_table_info=True)
