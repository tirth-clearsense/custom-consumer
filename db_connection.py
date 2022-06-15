# import psycopg2
# from postgresdb import *
from base_schema import base_schema
import copy
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# conn = psycopg2.connect(database="personicletest",user="tirth",password="password",host="localhost",port="5432")

def loadSession():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


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
