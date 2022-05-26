import psycopg2

conn = psycopg2.connect(database="personicletest",user="tirth",password="password",host="localhost",port="5432")

cursor = conn.cursor()

cursor.execute('''CREATE TABLE user_datastreams (individual_id TEXT,source TEXT, datastreams TEXT, last_updated TIMESTAMP, PRIMARY KEY(individual_id,source,last_updated));''')

conn.commit()
conn.close()