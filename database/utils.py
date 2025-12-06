import psycopg2
from database.config import config

DATABASE_CONFIG = config(filename='./database/database.ini')

def get_db():
    connection = psycopg2.connect(**DATABASE_CONFIG)
    return connection

def getURLs(tableName):
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(f"""SELECT url FROM {tableName}
                       ORDER BY id;""")
        
        return tuple(row[0] for row in cursor.fetchall())
    finally:
        conn.close()

if __name__ == '__main__':
    urls = getURLs('gameland')
    print(urls)
    print(len(urls))