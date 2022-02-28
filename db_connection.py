# import psycopg2

# conn = psycopg2.connect(database="personicletest",user="tirth",password="password",host="localhost",port="5432")

# cursor = conn.cursor()

# cursor.execute('''CREATE TABLE heartrate (individual_id TEXT, timestamp TIMESTAMP, source TEXT, value INT, unit TEXT, confidence REAL, PRIMARY KEY(individual_id, timestamp, source));''')

# conn.commit()
# conn.close()