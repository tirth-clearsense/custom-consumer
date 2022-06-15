# import psycopg2
# from postgresdb import *
from base_schema import base_schema
import copy
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# conn = psycopg2.connect(database="personicletest",user="tirth",password="password",host="localhost",port="5432")

# cursor = conn.cursor()
# postgres://lrngdobnzuypwd:98c995081e1dc92160f43f8a5c4d02a5e5ef5c4d893bda78c7dda4748a5510e4@ec2-3-222-204-187.compute-1.amazonaws.com:5432/dad10s07mobc6r
# cursor.execute('''CREATE TABLE user_datastreams (individual_id TEXT,source TEXT, datastreams TEXT, last_updated TIMESTAMP, PRIMARY KEY(individual_id,source,last_updated));''')
def loadSession():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

    
# conn.commit()
# conn.close()
# engine = create_engine("postgresql://lrngdobnzuypwd:98c995081e1dc92160f43f8a5c4d02a5e5ef5c4d893bda78c7dda4748a5510e4@ec2-3-222-204-187.compute-1.amazonaws.com:5432/dad10s07mobc6r")
engine = create_engine("postgresql://vaibhav@personicle-timescale-db:personicle2021!@personicle-timescale-db.postgres.database.azure.com/timescaledb")
Base = declarative_base(engine)

session = loadSession()

query = "select * from users;"

res = session.execute(query)
session.commit()
print(res.rowcount)
for user in res:
    print(user)

# model_class_user_datastreams = generate_table_class("users", copy.deepcopy(base_schema['user_info.avsc']))
