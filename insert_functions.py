import mysql.connector
from mysql.connector import Error


# Connect to BD
# —-------------------------------------------—
config = {
'user': 'root',
'password': '',
'host': 'localhost',
'database': 'my_twitter',
'raise_on_warnings': True
}

conn = mysql.connector.connect(**config)

# —--------------------------------------------—
# Connect to BD "twitchack"
# —--------------------------------------------—
config_twitchack = {
'user': 'root',
'password': '',
'host': 'localhost',
'database': 'twitter',
'raise_on_warnings': True
}

conn_twitchack = mysql.connector.connect(**config_twitchack)

# For insert new row , we use this function to insert posts and comments
def insert_row(query, data):
    try:
        cursor = conn.cursor()
        cursor.executemany(query, data)

        conn.commit()
    except Error as e:
        print('Error:', e)

    finally:
        cursor.close()


# Connectin for database twitchack
def insert_row_twitchack(query, data):
    try:
        cursor = conn_twitchack.cursor()
        cursor.executemany(query, data)

        conn.commit()
    except Error as e:
        print('Error:', e)

    finally:
        cursor.close()


def select_twitchack(query, data):
    try:
        cursor = conn_twitchack.cursor()
        cursor.execute(query, data)

        return cursor.fetchall()
    except Error as e:
        print('Error:', e)

    finally:
        cursor.close()