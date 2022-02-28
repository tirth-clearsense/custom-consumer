from flask import session
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from configparser import ConfigParser

config_object = ConfigParser()
database = config_object["CREDENTIALSDATABASE"]

engine = create_engine("postgresql://{username}:{password}@{dbhost}/{dbname}".format(username=database['USERNAME'], password=database['PASSWORD'],
                                                                                                        dbhost=database['HOST'], dbname=database['NAME']))

engine.execute('''CREATE TABLE IF NOT EXISTS heartrate (individual_id TEXT, timestamp TIMESTAMP, source TEXT, value INT, unit TEXT, confidence REAL, PRIMARY KEY(individual_id, timestamp, source));''')

Base = declarative_base(engine)
Base.metadata.reflect(engine)

class Heartrate(Base):
    __table__ = Base.metadata.tables['heartrate']

    def __repr__(self):
        return '''<Heartrate(individual_id='{0}', timestamp='{1}', source='{2}', value='{3}', unit='{4}', confidence='{5}')>'''.format(self.individual_id,
        self.timestamp, self.source, self.value, self.unit, self.confidence)

def loadSession():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

    