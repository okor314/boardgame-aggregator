import psycopg2
import re
from thefuzz import fuzz

from database.config import config
from database.site_database import removeChar


DATABASE_CONFIG = config(filename='./database/database.ini')
FUZZ_BARRIER = 60

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

def fuzzMatching(string, choises, minScore = FUZZ_BARRIER, scorer = fuzz.WRatio):
    """choises is lists of tuples (id, str)"""
    scores = ((id, choise, scorer(string, choise)) for id, choise in choises)
    bestChoises = list(filter(lambda score: score[2] >= minScore, scores))
    return max(bestChoises, key=lambda x: x[2], default=None) 


if __name__ == '__main__':
    urls = getURLs('gameland')
    print(urls)
    print(len(urls))