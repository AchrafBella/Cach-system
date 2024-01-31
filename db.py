import sqlite3
from math import log


def create_connection(db_file):
    """ create a database connection to the SQLite database specified by db_file
    :param db_file: database file name
    :return: Connection object or None
    """
    with sqlite3.connect(db_file) as conn:
        return conn


def create_table(conn):
    """ create a table called CACHE
    :param conn: Connection object
    """
    with conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS CACHE ( key BLOB,
                                                        insert_time DATETIME,
                                                        access_time DATETIME,
                                                        latency_gain_ms INTEGER,
                                                        data BLOB,
                                                        code_location TEXT,
                                                        perfyt_version TEXT)""")


def insert_data(conn, key, insert_time, access_time, latency_gain_ms, data, code_location, perfyt_version,
                current_time, high_threshold, low_threshold):
    """ insert an element in the table
    :param conn: Connection object
    :param key: the hashed parameters of the data
    :param insert_time: the time you insert the data
    :param access_time: the time you access to the data
    :param latency_gain_ms : time of executing the code
    :param data: the data under protobuf format
    :param code_location: location of the code
    :param perfyt_version: the version of programme
    :param current_time: the current time
    :param high_threshold: (bytes) when we reach to this limit we start release the database
    :param low_threshold: (bytes) we delete data that below this size
    """
    min_low_threshold = min(low_threshold, high_threshold - len(data))
    with conn:
        conn.create_function('ln', 1, log)

        if len(data) > high_threshold:
            raise Exception('The size of the data you want to insert is higher than the threshold')

        if get_cache_size(conn) + len(data) >= high_threshold:
            conn.execute("""DELETE FROM CACHE WHERE key in (SELECT c1.key FROM CACHE c1 INNER JOIN CACHE c2 on 
             ln((1/(@current_time - c1.access_time))) + 2 * ln(c1.latency_gain_ms) - ln(length(c1.data)) <= 
              ln((1/(@current_time - c2.access_time))) + 2 * ln(c2.latency_gain_ms) - ln(length(c2.data))
            GROUP BY c1.key HAVING SUM(length(c2.data)) >= @low_threshold)""", {"current_time": current_time,
                                                                                "low_threshold": min_low_threshold})
        add_data(conn, key, insert_time, access_time, latency_gain_ms, data, code_location, perfyt_version)


def add_data(conn, key, insert_time, access_time, latency_gain_ms, data, code_location, perfyt_version):
    """ function to add data
    :param conn: Connection object
    :param key: the hashed parameters of the data
    :param insert_time: the time you insert the data
    :param access_time: the time you access to the data
    :param latency_gain_ms : time of executing the code
    :param data: the data under protobuf format
    :param code_location: location of the code
    :param perfyt_version: the version of programme
    """
    with conn:
        conn.execute("""INSERT OR REPLACE INTO CACHE VALUES(@key, @insert_time, @access_time, @latency_gain_ms, @data, 
        @code_location, @perfyt_version)""", {"key": key, "insert_time": insert_time,
                                              "access_time": access_time,
                                              "latency_gain_ms": latency_gain_ms, "data": data,
                                              "code_location": code_location, "perfyt_version": perfyt_version})


def delete_data(conn, key):
    """delete an element in the table
    :param conn: Connection object
    :param key: contains the hash condition to execute the key
    """
    with conn:
        conn.execute("""DELETE FROM CACHE WHERE @key=?""", {"key": key})


def retrieve_data(conn, key):
    """ Query tasks by id
    :param conn: Connection object
    :param key: contains the hash condition
    :return list: tuple of data
    """
    with conn:
        return conn.execute("""SELECT data FROM CACHE WHERE key=@key""", {"key": key}).fetchone()


def get_data_information(conn):
    """
    :param conn: Connection object
    :return tuple of information about the data
    """
    with conn:
        return conn.execute("""SELECT insert_time, access_time, latency_gain_ms, length(data) FROM CACHE""").fetchone()


def get_cache_size(conn):
    """ get the size of the table
    :param conn: Connection object
    """
    with conn:
        size = conn.execute("""SELECT SUM(length(data)) FROM CACHE""").fetchone()[0]
        if size is None:
            return 0
        return size


def get_size_elements(conn):
    """ get the size of an element in the table
    :param conn: Connection object
    """
    with conn:
        return conn.execute("""SELECT key, length(data) FROM CACHE""").fetchall()


def get_elements_count(conn):
    """ how many rows we have in total
    :param conn: Connection object
    :return: return the number of rows
    """
    with conn:
        return conn.execute("""SELECT COUNT(*) FROM CACHE""").fetchone()[0]


def update_access_time(conn, now, key):
    """
    :param conn: Connection object
    :param now: time object now
    :param key: the key
    """
    with conn:
        conn.execute("""UPDATE CACHE SET access_time=@now WHERE key=@key""", {"now": now, "key": key})


def display_data(conn):
    """ view the table
    :param conn: Connection object
    """
    with conn:
        return conn.execute("""SELECT * FROM CACHE""").fetchall()


def clear_table(conn):
    """ delete the table
    :param conn: Connection object
    """
    with conn:
        conn.execute("""DROP TABLE CACHE""")