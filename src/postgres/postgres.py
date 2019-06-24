import psycopg2

connection = psycopg2.connect(host='ec2-34-239-95-229.compute-1.amazonaws.com',
                              database='test',
                              user='postgres', password='$password')

cursor = connection.cursor()

cursor.execute("INSERT INTO links (article_id, link_name, first_time_stamp, deleted_time_stamp) " +
               "VALUES " + "(0, 'Hello World', '2010-08-25 01:11:11', '2017-04-30 13:37:40')")

connection.commit()

print("POSTGRESQL DONE")
