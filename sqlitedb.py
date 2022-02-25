from importlib.metadata import metadata
import sqlalchemy as db 
import sqlite3

connection = sqlite3.connect('test.db')

cursor = connection.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS Heartrates (individual_id TEXT, timestamp TIMESTAMP, source TEXT, value INT, unit TEXT, confidence REAL, PRIMARY KEY(individual_id, timestamp, source))''')

connection.commit()
connection.close()

engine = db.create_engine('sqlite:///test.db')

conn = engine.connect()

metadata = db.MetaData()

heartrate = db.Table('Heartrates', metadata, autoload=True, autoload_with=engine)