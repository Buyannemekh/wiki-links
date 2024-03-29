import os


class PostgresConnector(object):
    def __init__(self):
        self.hostname = os.environ["POSTGRES_HOSTNAME"]
        self.database_name = os.environ["POSTGRES_DBNAME"]
        self.url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(hostname=self.hostname, db=self.database_name)
        self.properties = {
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver"
        }

    # def write(self, df, table, mode):
    #     df.jdbc(self.url_connect, table, mode, self.properties)

    # write link and count data frame from batch_process to postgres
    def write_pages_to_postgres(self, df_pages):
        df_pages.select('page_id', 'page_title', 'time_stamp', 'links', 'link_cnt').\
            write.jdbc(url=self.url_connect,
                       table='pages',
                       properties=self.properties,
                       mode='append')

        print("PAGES DONE")
