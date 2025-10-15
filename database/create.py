from sqlalchemy import create_engine
from config import config
import pandas as pd

from collections.abc import Iterable

# params = config(filename='./database/database.ini')
# engine = create_engine(f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}/{params['database']}")
# conn = engine.connect()
# conn.close()

def removeChar(string: str, char):
    if isinstance(char, str):
        return string.replace(char, '')
    elif isinstance(char, Iterable):
        for c in char:
            string = string.replace(c, '')
        return string
    
print(removeChar('fdfhf88g47g', ['0']))