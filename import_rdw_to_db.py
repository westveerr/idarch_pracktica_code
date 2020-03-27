import csv
import datetime
import sqlalchemy as sa

from columns import COLUMN_INDEX, DB_COLUMNS

BLOCK_SIZE = 1024

PATH_SMALL_SAMPLE = r"C:\Users\barthel.m\Dropbox\Werk HL\Data\ibuii\2018-2019\RDW_Passenger_Cars SAMPLE 50.csv"
PATH_SAMPLE = r"C:\Users\barthel.m\Dropbox\Werk HL\Data\ibuii\2018-2019\RDW_Passenger_Cars SAMPLE 55K.csv"
PATH_FULL = r"C:\Users\barthel.m\Dropbox\Werk HL\Data\ibuii\2018-2019\RDW_Passenger_Cars.csv"

DB_URL = "postgresql://postgres:pw@localhost:5432/RDWChallenge"

start_time = datetime.datetime.now()


def element_by(column_header, row):
    result = row[COLUMN_INDEX[column_header]]

    if result == "":
        return None
    else:
        return result


def params_dict_from(row):
    result = dict()

    for column_header in DB_COLUMNS:
        result[column_header] = element_by(column_header, row)

    return result


error_count = 0
success_count = 0
def insert_block(values_list):
    global  error_count, success_count
    insert = TABLE_CAR.insert()
    try:
        connection.execute(insert, values_list)
        success_count += len(values_list)
    except Exception:
        for values in values_list:
            try:
                connection.execute(insert, values)
                success_count += 1
            except Exception as ex:
                error_count += 1
                print(ex)
                print(values)


engine = sa.create_engine(DB_URL)

META_DATA = sa.MetaData(bind=engine)
META_DATA.reflect()
TABLE_CAR = META_DATA.tables["Car"]

connection = engine.connect()
connection.execute(TABLE_CAR.delete())
with open(PATH_FULL, encoding="UTF-8") as file:
    csv_reader = csv.reader(file)
    header = next(csv_reader)
    print(header)
    counter = 0
    values_list = []
    for row in csv_reader:
        values = params_dict_from(row)
        values_list.append(values)
        if counter % BLOCK_SIZE == 0:
            insert_block(values_list)
            values_list = []
            print(counter)
        counter += 1
    insert_block(values_list)


end_time = datetime.datetime.now()
print("Aantal rijen gelezen in bestand: ", counter)
print("Aantal rijen ge-insert: ", success_count)
print("Aantal fouten: ", error_count)
print("Tijd: ", end_time - start_time)
