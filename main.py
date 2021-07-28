from typing import Optional, IO, Tuple
from collections import namedtuple
from prefect import task, Flow
import mysql.connector


# Origin Database
# Parameters that can NOT be changed
file_full_path: Optional[str] = None
file_is_open: Optional[bool] = None
file_reference: Optional[IO] = None
Estado = namedtuple('Estado', 'sigla, data, totalObitos')
data_transformed = []

# Destiny Database variables
mydb: Optional[mysql.connector.connection_cext.CMySQLConnection] = None
host = 'localhost'
username = 'root'
password = 'example'
default_database = 'covid19'

# Destiny Database manipulation variables
mycursor: Optional[mysql.connector.connection_cext.CMySQLCursor] = None
table_insertion_name = 'totalObitos'

# Parameters that can be changed
# [OPTIONAL] skip_column_names -> Skip columns line
#   Options: True or None, Default None
# [OPTIONAL] files_path -> Specify the path to file: Default None
# [MANDATORY] file_name -> Specify the file name
skip_column_names: Optional[bool] = True
files_path: Optional[str] = None
file_name: str = 'HIST_PAINEL_COVIDBR_2021_Parte2_26jul2021.csv'
delimiter = ';'


# Try open file
# If does't raise error

def return_file():
    global file_reference
    global file_is_open
    try:
        file_reference = open(file_full_path, "r")
        file_is_open = True
    except FileExistsError as error:
        print(error)


# Set the file full path

def set_file_full_path():
    global file_full_path
    if not files_path:
        file_full_path = file_name
    else:
        file_full_path = str(files_path) + file_name


# Try connect mysql
# If does't raise error

def connect_mysql():
    global mydb
    mydb = mysql.connector.connect(
        host=host,
        user=username,
        password=password,
        database=default_database
    )


# Sets cursor to handle with database
def set_mycursor():
    global mydb
    global mycursor
    if mydb:
        mycursor = mydb.cursor()


# Prepare values to load in database

def prepare_load_statement(line: Estado) -> Tuple:
    values = (line.sigla, line.data, line.totalObitos)
    return values


# Execute load in database
def execute_load(values):
    global mycursor
    global mydb
    if values:
        default_statement = f"INSERT INTO {table_insertion_name} (estado, data, totalObitos) VALUES (%s, %s, %s)"
        mycursor.execute(default_statement, values)
        mydb.commit()


# Validade data extracted
def is_validate_data(registry):
    strip_registry = registry.split(delimiter)
    if strip_registry != ['']:
        if strip_registry[1] != '' and strip_registry[7] == '2021-07-26' and strip_registry[2] == '':
            return True
        else:
            return False
    return False


# Extract Information from source
@task()
def extract() -> list:
    # Checks if origin file is open.
    if not file_is_open:
        # If doesn't, open it
        return_file()
        # Checks if option to skip first line is enable
        # If enable skip first line and read line
        # Else just read line
        if skip_column_names:
            next(file_reference)
            registry = file_reference.readline().strip()
        else:
            registry = file_reference.readline().strip()
    else:
        registry = file_reference.readline().strip()
    # Validate de line extracted from file
    while not is_validate_data(registry):
        # If doesn't valid, read next line
        registry = file_reference.readline().strip()
        # If line read is 'EOF' terminate execution
        if registry == '':
            exit()
    return registry.split(delimiter)


# Transform Information
@task()
def transform(registry: list):
    registry = Estado(registry[1], registry[7], registry[12])
    return registry


# Load Data into database
@task()
def load(registry: Estado):
    load_statement = prepare_load_statement(registry)
    execute_load(load_statement)


def execute_flow() -> Flow:
    with Flow("etl") as floww:
        data = extract()
        d2 = transform(data)
        load(d2)
    return floww


if __name__ == "__main__":

    set_file_full_path()
    connect_mysql()
    set_mycursor()

    flow = execute_flow()
    flow.visualize()
    while True:
        flow.run()
