import sys

import sqlalchemy

import first_data as fd

import pprint

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

    result = connection.execute(
        """SELECT name, release_year 
            FROM albums 
            WHERE release_year = 2018;""").fetchall()
    print(f'request 1\n{result}\n')

    result = connection.execute(
        """SELECT name, ROUND(track_length / 60, 2) 
            FROM tracks 
            ORDER BY track_length DESC;""").fetchone()
    print(f'request 2\n{result}\n')

    result = connection.execute(
        """SELECT name, ROUND(track_length / 60, 2) 
            FROM tracks 
            WHERE track_length >= 3.5 * 60;""").fetchall()
    print(f'request 3\n{result}\n')

    result = connection.execute(
        """SELECT name, release_year 
            FROM music_collections 
            WHERE release_year BETWEEN 2018 AND 2020;""").fetchall()
    print(f'request 4\n{result}\n')

    result = connection.execute(
        """SELECT name 
            FROM singers 
            WHERE name NOT LIKE '%% %%';""").fetchall()
    print(f'request 5\n{result}\n')

    result = connection.execute(
        """SELECT name 
            FROM tracks 
            WHERE name LIKE '%%My%%' or name LIKE '%%my%%' or name LIKE '%%мой%%';""").fetchall()
    print(f'request 6\n{result}\n')

def execute_requests_join(connection):

    result = connection.execute(
        """SELECT name, count(singer_id) genre_q 
            FROM genres g 
            JOIN genressingers gs ON g.id = gs.genre_id 
            GROUP BY g.name 
            ORDER BY genre_q desc;""").fetchall()
    print(f'request 2.1\n{result}\n')

    result = connection.execute(
        """SELECT name, count(track_id) collection_q 
            FROM music_collections mc
            JOIN collectiontracks c ON mc.id = c.collection_id
            WHERE release_year between 2019 AND 2020
            GROUP BY mc.name
            ORDER BY collection_q desc;""").fetchall()
    print(f'request 2.2\n{result}\n')

    result = connection.execute(
        """SELECT a.name AS a_name, ROUND(avg(track_length), 2) albums_q 
            FROM albums a 
            JOIN tracks t ON a.id = t.album_id  
            GROUP BY a_name 
            ORDER BY albums_q;""").fetchall()
    print(f'request 2.3\n{result}\n')

    result = connection.execute(
        """SELECT s.name singers_q 
            FROM singers s 
            JOIN albumssingers a ON s.id = a.singer_id 
            JOIN albums al ON a.album_id = al.id 
            WHERE s.name <> (
                SELECT s.name singers_q FROM singers s 
                JOIN albumssingers a ON s.id = a.singer_id 
                JOIN albums al ON a.album_id = al.id 
                WHERE al.release_year = 2020)
            GROUP BY s.name;""").fetchall()
    print(f'request 2.4\n{result}\n')# Альбом в 2020 году есть только у Yello

    result = connection.execute(
        """SELECT mc.name collection_q 
            FROM music_collections mc  
            JOIN collectiontracks c ON mc.id = c.collection_id  
            JOIN tracks t ON c.track_id = t.id 
            JOIN albumssingers a ON t.album_id = a.album_id  
            JOIN singers s ON a.singer_id = s.id 
            WHERE s.name  = 'Yello'
            GROUP BY mc.name 
            ORDER BY collection_q;""").fetchall()
    print(f'request 2.5\n{result}\n')

    result = connection.execute(
        """SELECT a.name 
            FROM albums a
            JOIN albumssingers als ON a.id = als.album_id
            JOIN singers s ON als.singer_id = s.id
            JOIN genressingers g ON s.id = g.singer_id 
            JOIN genres grs ON g.genre_id = grs.id 
            GROUP BY a.name 
            HAVING COUNT(DISTINCT grs.name) > 1
                ORDER BY a.name; """).fetchall()
    print(f'request 2.6\n{result}\n')

    result = connection.execute(
        """SELECT t.name 
            FROM tracks as t
            LEFT JOIN collectiontracks ct ON t.id = ct.track_id
            WHERE ct.track_id IS NULL;""").fetchall()
    print(f'request 2.7\n{result}\n')

    result = connection.execute(
        """SELECT s.name, t.track_length 
            FROM tracks as t
            JOIN albums a ON a.id = t.album_id
            JOIN albumssingers alsin ON alsin.album_id = a.id
            JOIN singers s ON s.id = alsin.singer_id
            GROUP BY s.name, t.track_length
            HAVING t.track_length = (SELECT MIN(track_length) FROM tracks)
            ORDER BY s.name;""").fetchall()
    print(f'request 2.8\n{result}\n')

    result = connection.execute(
        """SELECT DISTINCT a.name
            FROM albums a
            JOIN tracks t ON t.album_id = a.id
            WHERE t.album_id IN (
                SELECT album_id
                FROM tracks
                GROUP BY album_id
                HAVING count(id) = (
                    SELECT count(id)
                    FROM tracks
                    GROUP BY album_id
                    ORDER BY count
                    LIMIT 1
                )
            )
            ORDER BY a.name;""").fetchall()
    print(f'request 2.8\n{result}\n')


if __name__ == '__main__':

    connection = connect_to_db()

    # Заполнение таблиц
    # fill_tables(connection)

    # Запросы select
    execute_requests(connection)

    # Запросы select + join
    execute_requests_join(connection)
