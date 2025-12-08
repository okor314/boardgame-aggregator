import psycopg2
from thefuzz import fuzz, process

from database.config import config
from database.utils import fuzzMatching


DATABASE_CONFIG_PATH = './database/database.ini'


def createGameTable(connection):
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS test (
                id SERIAL PRIMARY KEY,
                title TEXT,
                min_players SMALLINT,
                max_players SMALLINT,
                age SMALLINT,
                maker TEXT,
                bgg_id INT,
                gameland_id INT,
                geekach_id INT,
                woodcat_id INT
                );""")
    connection.commit()
    cursor.close()

def updateByBBG(tableName, connection):
    # Abort if bgg_id is not exists
    if not isColumnExists(tableName, 'bgg_id', connection):
        return
    
    cursor = connection.cursor()

    # Selecting rows with id not in game table
    _, rows = getMissingRows(tableName, ['id', 'bgg_id'], connection)

    if not rows: return     # Abort if all items from table alredy in game table

    for row in rows:
        table_id = row[0]
        bgg_id = row[1]
        if bgg_id is None: continue
        
        cursor.execute(f"""SELECT id, title, min_players, max_players, age, maker
                        FROM {tableName}
                        WHERE id = {table_id};""")
        features = cursor.fetchone()
        game_choises = getGamesWithBBG(tableName, bgg_id, connection)
        match = chooseOne(tableName, features, connection, game_choises=game_choises)
        
        if not match: continue
        game_id = match[0]

        cursor.execute(f"""UPDATE test
                    SET {tableName}_id = {table_id}
                    WHERE id = {game_id};""")
    connection.commit()
    cursor.close()

def updateByFeatures(tableName, connection, rows: list = []):
    cursor = connection.cursor()
    # If rows not provided
    if not rows:
        # Selecting rows with id not in game table
        _, rows = getMissingRows(tableName, ['id', 'title', 'min_players', 'max_players', 'age', 'maker'], connection)

    if not rows: return     # Abort if all items from table alredy in game table

    for row in rows:
        table_id = row[0]
        match = chooseOne(tableName, row, connection)
        if match:
            cursor.execute(f"""UPDATE test
                        SET {tableName}_id = {table_id}
                        WHERE id = {match[0]};""")
            connection.commit()
    cursor.close()  

 

def insertOneRow(tableName: str, columns: list, row: tuple, connection):
    cursor = connection.cursor()
    columns = [f"{tableName}_id" if col=='id' else col for col in columns]
    selectedColumns = ','.join(columns)
    values = ','.join('%s' for col in columns)
    cursor.execute(f"""INSERT INTO test
                   ({selectedColumns})
                   VALUES ({values});""", row)
    connection.commit()
    cursor.close()

def insertRows(tableName: str, columns: list, rows: list, connection):
    for row in rows:
        insertOneRow(tableName, columns, row, connection)

def isColumnExists(tableName: str, column: str, connection) -> bool:
    cursor = connection.cursor()
    cursor.execute(f"""SELECT column_name 
                   FROM information_schema.columns 
                   WHERE table_name='{tableName}' and column_name='{column}';""")
    result = cursor.fetchone()
    cursor.close()
    return bool(result)

def getMissingRows(tableName: str, columns: list, connection):
    selectedColumns = 't.*'
    if columns:
        # Reove non-existing columns and format for query
        columns = [column for column in columns if isColumnExists(tableName, column, connection)]
        selectedColumns = ','.join(['t.'+c for c in columns])
        
    cursor = connection.cursor()
    cursor.execute(f"""SELECT {selectedColumns} 
                   FROM {tableName} as t
                   LEFT JOIN test
                   ON t.id = test.{tableName}_id
                   WHERE test.{tableName}_id IS NULL
                   ORDER BY t.id""")
    rows = cursor.fetchall()
    cursor.close()

    return columns, rows

def isequal(columnName, value):
    """Return (SQL condition string, [param]) for safe parameterized query."""
    if value is None:
        return f"{columnName} IS NULL", []
    else:
        return f"{columnName} = %s", [value]

def chooseOne(tableName, table_row: tuple, connection, game_choises: list = None):
    """Safely select candidate games with matching numeric/text features."""
    cursor = connection.cursor()
    table_title = table_row[1]
    table_min = table_row[2]
    table_max = table_row[3]
    table_age = table_row[4]
    table_maker = table_row[5]

    choises = game_choises
    if game_choises is None:
        conditions = []
        params = []

        # Build conditions safely
        for col, val in [
            ('min_players', table_min),
            ('max_players', table_max),
            ('age', table_age),
            ('maker', table_maker)
        ]:
            cond, param = isequal(col, val)
            conditions.append(cond)
            params.extend(param)

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT id, title FROM test
            WHERE {where_clause}
              AND {tableName}_id IS NULL
            ORDER BY id;
        """

        cursor.execute(query, params)
        choises = cursor.fetchall()

    match = fuzzMatching(table_title, choises)
    cursor.close()

    return match

def getGamesWithBBG(tableName, bgg_id, connection):
    cursor = connection.cursor()
    cursor.execute(f"""SELECT id, title FROM test
                WHERE bgg_id = {bgg_id}
                AND {tableName}_id IS NULL
                ORDER BY id;""")
    result = cursor.fetchall()
    cursor.close()
    return result

def createConnections(tableName, connection):
    createGameTable(connection)
    updateByBBG(tableName, connection)
    updateByFeatures(tableName, connection)

    # If there any rows in tableName left not connected to
    # table game, inserting them
    columns, missingRows = getMissingRows(tableName,
                                 ['id', 'title', 'min_players', 'max_players', 'age', 'maker', 'bgg_id'],
                                 connection)
    insertRows(tableName, columns, missingRows, connection)

def updateGameTable(connection):
    cursor = connection.cursor()
    cursor.execute("""SELECT name FROM site
                   ORDER BY id;""")
    names = [row[0] for row in cursor.fetchall()]
    cursor.close()

    for name in names:
        createConnections(name, connection)

if __name__ == "__main__":
    params = config(DATABASE_CONFIG_PATH)
    conn = psycopg2.connect(**params)
    import time
    start = time.time()
    updateGameTable(conn)
    end = time.time()
    print(end - start)
    conn.close()
    pass

