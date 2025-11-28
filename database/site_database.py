import psycopg2
from sqlalchemy import create_engine, text
from config import config
import pandas as pd

from collections.abc import Iterable

DATABASE_CONFIG = config(filename='./database/database.ini')

def removeChar(string: str, char):
    if not isinstance(string, str):
        return string
    elif isinstance(char, str):
        return string.replace(char, '')
    elif isinstance(char, Iterable):
        for c in char:
            string = string.replace(c, '')
        return string
    
def replaceChar(string: str, translator: dict):
    if not isinstance(string, str):
        return string
    else:
        for old, new in translator.items():
            string = string.replace(old, new)
        return string
    
def getMin(string):
    if not isinstance(string, str):
        return string
    else:
        string = removeChar(string, ['+', ' '])
        string = replaceChar(string, {',':'-', ';':'-', '–':'-'})
        return int(string.split('-')[0])
        

def getMax(string):
    if not isinstance(string, str):
        return string
    else:
        string = removeChar(string, ['+', ' '])
        string = replaceChar(string, {',':'-', ';':'-', '–':'-'})
        try:
            return int(string.split('-')[1])
        except:
            return pd.NA
        
def prepareDataFrame(df: pd.DataFrame):
    df = df.drop_duplicates('url', keep='last')
    df['min_players'] = df.players.apply(getMin).astype('Int8')
    df['max_players'] = df.players.apply(getMax).astype('Int8')
    df['age'] = df.age.apply(getMin).astype('Int8')
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
    bbg_column = 'bbg_url TEXT,' if 'bbg_url' in df.columns else ''

    conn = psycopg2.connect(**DATABASE_CONFIG)
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS {name}')
    conn.commit()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {name} (
                id          SERIAL PRIMARY KEY,
                {name}_id   TEXT,
                title       TEXT,
                in_stock    TEXT,
                price       REAL,
                min_players SMALLINT,
                max_players SMALLINT,
                age         SMALLINT,
                maker       TEXT,
                url         TEXT UNIQUE,
                {bbg_column}
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
    connection.commit()


if __name__ == "__main__":
    params = DATABASE_CONFIG
    engine = create_engine(f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}/{params['database']}")
    conn = engine.connect()
    

    conn.close()
    pass
