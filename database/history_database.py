import psycopg2
from config import config

def createHistoryTable(connection):
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS history (
                   game_id INT,
                   site_id SMALLINT,
                   price REAL,
                   checkdate DATE);""")
    connection.commit()
    cursor.close()

if __name__ == "__main__":
    params = config('./database/database.ini')
    conn = psycopg2.connect(**params)
    createHistoryTable(conn)
    conn.close()