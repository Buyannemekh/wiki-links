import psycopg2

connection = psycopg2.connect(host='ec2-34-239-95-229.compute-1.amazonaws.com',
                              database='test',
                              user='postgres', password='$password')

cursor = connection.cursor()

cursor.execute("INSERT INTO links (article_id, link_name, first_time_stamp, deleted_time_stamp) " +
               "VALUES " + "(0, 'Hello World', 2006-11-09T06:43:10Z, 2018-06-19T05:56:36Z)")

connection.commit()

print("POSTGRESQL DONE")
