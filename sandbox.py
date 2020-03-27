import sqlalchemy as sa

DB_URL = "postgresql://postgres:pw@localhost:5432/RDWChallenge"

engine = sa.create_engine(DB_URL)
meta_data = sa.MetaData(bind=engine)
meta_data.reflect()
table_car = meta_data.tables["Car"]
insert = table_car.insert()
print(insert)
