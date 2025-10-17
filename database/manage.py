import psycopg2
from config import config

DATABASE_CONFIG = config(filename='./database/database.ini')

conn = psycopg2.connect(**DATABASE_CONFIG)
cur = conn.cursor()

def createGameTable(connection):
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS game (
                id SERIAL PRIMARY KEY,
                title TEXT,
                min_players SMALLINT DEFAULT NULL,
                max_players SMALLINT DEFAULT NULL,
                age SMALLINT DEFAULT NULL,
                bbg_url TEXT DEFAULT NULL,
                gameland_id INT,
                geekach_id INT,
                woodcat_id INT
                );""")
    connection.commit()
    cursor.close()

def updateByBBG(tableName, connection):
    cursor = connection.cursor()

    # Selecting rows with id not in game table
    cursor.execute(f"""SELECT t.id, t.bbg_url 
            FROM {tableName} AS t
            LEFT JOIN game
            ON t.id = game.{tableName}_id
            WHERE game.{tableName}_id IS NULL
            ORDER BY t.id;""")
    
    rows = cursor.fetchall()
    if not rows: return     # Abort if all items from table alredy in game table

    for row in rows:
        table_id = row[0]
        bbg_url = row[1]

        cursor.execute(f"""SELECT id FROM game
                WHERE bbg_url = '{bbg_url}'""")
        game_id = cursor.fetchall()
        
        if not game_id: continue
        game_id = game_id[0][0]

        cursor.execute(f"""UPDATE game
                    SET {tableName}_id = {table_id}
                    WHERE id = {game_id}""")
    connection.commit()




cur.close()
conn.close()

