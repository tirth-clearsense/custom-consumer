from flask import session
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('postgresql://tirth:password@localhost/personicletest')
Base = declarative_base(engine)
Base.metadata.reflect(engine)

class Heartrate(Base):
    __table__ = Base.metadata.tables['heartrates']

    def __repr__(self):
        return '''<Heartrate(individual_id='{0}', timestamp='{1}', source='{2}', value='{3}', unit='{4}', confidence='{5}')>'''.format(self.individual_id,
        self.timestamp, self.source, self.value, self.unit, self.confidence)

def loadSession():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

    