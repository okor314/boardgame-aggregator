from database.utils import get_db

def createHistoryTable(connection):
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS history (
                   game_id INT,
                   site_id SMALLINT,
                   price REAL,
                   checkdate DATE,
                   PRIMARY KEY (game_id, site_id, checkdate));""")
    connection.commit()
    cursor.close()

def updateHistoryTable(connection):
    connection = None
    try:
        connection = get_db()
        createHistoryTable(connection)
        
        cursor = connection.cursor()
        # Get table names with site data
        cursor.execute("""SELECT id, name FROM site
                    ORDER BY id;""")
        sites = cursor.fetchall()

        for site_id, tableName in sites:
            cursor.execute(f"""INSERT INTO history (game_id, site_id, price, checkdate)
                            SELECT game.id, {site_id}, t.price, t.lastchecked FROM game
                            INNER JOIN {tableName} AS t
                            ON game.{tableName}_id = t.id
                            ORDER BY game.id
                            ON CONFLICT(game_id, site_id, checkdate)
                            DO NOTHING; """)
        connection.commit()
        cursor.close()
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    pass