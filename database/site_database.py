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

    df = df.drop('players', axis='columns')

    # Reordering columns
    columns = list(df.columns)
    columns = columns[:4] + ['min_players', 'max_players'] + columns[4:-2]
    df = df[columns]
    return df
    
def createTable(name, connection, pathToCSV, if_exists='fail'):
    df = pd.read_csv(pathToCSV)
    df = prepareDataFrame(df)

    df.to_sql(name, connection, index=False, if_exists=if_exists)

    # Renaming id from site and adding database selfincremental id
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cur = conn.cursor()

    cur.execute(f"""ALTER TABLE {name}
                RENAME COLUMN id TO {name}_id""")
    cur.execute(f"""ALTER TABLE {name}
                ADD COLUMN id SERIAL PRIMARY KEY""")
    # Specifying that url must be uniqe
    cur.execute(f"""ALTER TABLE {name} 
                ADD CONSTRAINT constraint_name UNIQUE (url);""")
    conn.commit()
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
                       price = excluded.price;""")
    connection.execute(query)
    connection.execute(text(f'DROP TABLE trans_{tableName};'))
    connection.commit()


if __name__ == "__main__":
    # params = DATABASE_CONFIG
    # engine = create_engine(f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}/{params['database']}")
    # conn = engine.connect()
    
    # # createTable('gameland', conn, './data/gameland_data.csv', if_exists='replace')
    # # createTable('geekach', conn, './data/geekach_data.csv', if_exists='replace')
    # # createTable('woodcat', conn, './data/woodcat_data.csv', if_exists='replace')

    # conn.close()
    pass
