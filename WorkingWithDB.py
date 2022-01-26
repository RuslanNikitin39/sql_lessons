import sys

import sqlalchemy

import first_data as fd

def connect_to_db():

    dialect_driver = 'postgresql'
    user = 'py47'
    password = '159456'
    host_port = 'localhost:5432'
    db = 'music_db2'

    engine = sqlalchemy.create_engine(f'{dialect_driver}://{user}:{password}@{host_port}/{db}')

    try:
        connection = engine.connect()
        print('Connection successful')
        return connection
    except:
        print('Connection is failed')
        sys.exit(0)

def insert_record_to_table(connection, table, record):
    try:
        connection.execute(f"""INSERT INTO {table} VALUES{record};""")
        print(f'Запись добавлена в таблицу {table}')
    except:
        print(f'Ключ "(id)={record[0]}" в таблице {table} уже существует.')

def fill_tables(connection):

    dict_data = fd.dict_data

    for key, value in dict_data.items():
        for item in value:
            insert_record_to_table(connection, key, item)

def execute_requests(connection):

    result = connection.execute("""SELECT name, release_year FROM albums WHERE release_year = 2018;""").fetchall()
    print(f'request 1\n{result}\n')

    result = connection.execute(
        """SELECT name, ROUND(track_length / 60, 2) FROM tracks ORDER BY track_length DESC;""").fetchone()
    print(f'request 2\n{result}\n')

    result = connection.execute(
        """SELECT name, ROUND(track_length / 60, 2) FROM tracks WHERE track_length >= 3.5 * 60;""").fetchall()
    print(f'request 3\n{result}\n')

    result = connection.execute(
        """SELECT name, release_year FROM music_collections WHERE release_year BETWEEN 2018 AND 2020;""").fetchall()
    print(f'request 4\n{result}\n')

    result = connection.execute(
        """SELECT name FROM singers WHERE name NOT LIKE '%% %%';""").fetchall()
    print(f'request 5\n{result}\n')

    result = connection.execute(
        """SELECT name FROM tracks WHERE name LIKE '%%My%%' or name LIKE '%%my%%' or name LIKE '%%мой%%';""").fetchall()
    print(f'request 6\n{result}\n')

if __name__ == '__main__':

    connection = connect_to_db()

    # Заполнение таблиц
    fill_tables(connection)

    # Запросы select
    execute_requests(connection)