from sqlalchemy import create_engine, text
from database.config import config
from database import utils
import pandas as pd
import time



DATABASE_URL = config(return_url=True)

def getConnection(retries=5, delay=5):
    engine = create_engine(DATABASE_URL, 
                           connect_args={"connect_timeout": 15})
    
    for attempt in range(retries):
        try:
            return engine.connect()
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
    
def prepareDataFrame(df: pd.DataFrame):
    df = df.drop_duplicates('url', keep='last')
    df['min_players'] = df.players.apply(utils.getMin)
    df['max_players'] = df.players.apply(utils.getMax)
    df['age'] = df.age.apply(utils.getMin)
    df['maker'] = df.maker.apply(lambda x: x.lower().replace(' ', '') if isinstance(x, str) else x)

    df = df.drop('players', axis='columns')

    # Reordering columns
    columns = list(df.columns)
    columns = columns[:4] + ['min_players', 'max_players'] + columns[4:-2]
    df = df[columns]
    return df
    
def _createTable(name, connection, pathToCSV):
    """This function will DROP your existing table.
    Do not use it if you don't want to lose your data"""
    df = pd.read_csv(pathToCSV)
    df = prepareDataFrame(df)
    df.rename(columns={'id': f'{name}_id'})
    # Definition of bbg column in SQL if it's present in dataframe
    bgg_column = 'bgg_id INTEGER,' if 'bgg_id' in df.columns else ''

    conn = utils.get_db()
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS {name}')
    conn.commit()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {name} (
                id          SERIAL PRIMARY KEY,
                game_id     INT,
                {name}_id   TEXT,
                title       TEXT,
                in_stock    BOOLEAN,
                price       REAL,
                min_players SMALLINT,
                max_players SMALLINT,
                age         SMALLINT,
                maker       TEXT,
                url         TEXT UNIQUE,
                {bgg_column}
                lastchecked DATE DEFAULT current_date);""")
    conn.commit()
    # Adding values to new table
    upsertTable(name, connection, pathToCSV)
    cur.close()
    conn.close()

def upsertTable(tableName, connection, pathToNewData):
    transdf = pd.read_csv(pathToNewData)
    transdf = prepareDataFrame(transdf)
    transdf.to_sql(f'trans_{tableName}', connection, if_exists='replace', index=False)

    columns = [f'{tableName}_id' if col=='id' else col for col in transdf.columns]
    selectedColumns = ','.join(columns)
    transColumns = ','.join(transdf.columns)

    query = text(f"""INSERT INTO {tableName} ({selectedColumns})
                       SELECT {transColumns} FROM trans_{tableName}
                       ON CONFLICT(url)
                       DO UPDATE SET
                       in_stock = excluded.in_stock,
                       price = excluded.price,
                       lastchecked = current_date;""")
    connection.execute(query)
    connection.execute(text(f'DROP TABLE trans_{tableName};'))

    connection.execute(text(f"""DELETE FROM {tableName}
                            WHERE title is NULL;"""))
    connection.commit()


if __name__ == "__main__":
    conn = getConnection()

    conn.close()
    pass
