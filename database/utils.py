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
    scores = ((id, choise, compareTitles(string, choise, scorer)) for id, choise in choises)
    bestChoises = list(filter(lambda score: score[2] >= minScore, scores))
    return max(bestChoises, key=lambda x: x[2], default=None)

def compareTitles(title_1: str, title_2: str, scorer = fuzz.WRatio) -> int:
    eng_1 = extract_english_fragments(title_1)
    eng_2 = extract_english_fragments(title_2)
    if len(eng_1) > 2 and len(eng_2) > 2:
        eng_score = scorer(eng_1, eng_2)
    else:
        eng_score = 0
    
    # If one of titles does not contain english words
    ukr_1 = extract_ukrainian_fragments(title_1)
    ukr_2 = extract_ukrainian_fragments(title_2)
    if len(ukr_1) > 2 and len(ukr_2) > 2:
        ukr_score = scorer(ukr_1, ukr_2)
    else:
        ukr_score = 0
    
    # If one of titles also does not contain ukrainian words
    # Compare full titles
    if eng_score and ukr_score:
        return (eng_score + ukr_score) / 2
    else:
        return scorer(title_1, title_2)

def extract_english_fragments(title: str) -> str:
    fragments = re.findall(r'([A-Za-z][A-Za-z0-9 ":\-\.,!\?]+)', title)
    if fragments:
        return ''.join(fragments)
    
    return ''

def extract_ukrainian_fragments(title: str) -> str:
    fragments = re.findall(r'([А-Яа-яІіЇїЄєҐґ][А-Яа-яІіЇїЄєҐґ0-9 ":\-\.,!\?]+)', title)
    if fragments:
        return ''.join(fragments)
    
    return ''

if __name__ == '__main__':
    urls = getURLs('gameland')
    print(urls)
    print(len(urls))