import time
import psycopg2
import re
from thefuzz import fuzz
from pandas import NA
from collections.abc import Iterable

from database.config import config


DATABASE_CONFIG = config()
FUZZ_BARRIER = 60

def get_db(retries=5, delay=5):
    for attempt in range(retries):
        try:
            return psycopg2.connect(
                **DATABASE_CONFIG,
                connect_timeout=15
            )
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(delay)

def getURLs(tableName):
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(f"""SELECT url FROM {tableName}
                       ORDER BY id;""")
        
        return tuple(row[0] for row in cursor.fetchall())
    except:
        return tuple()
    finally:
        if conn:
            conn.close()

def getSitesToScrape(limit=1) -> list[str]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""SELECT name FROM site
                        ORDER BY last_scraped_at NULLS FIRST
                        LIMIT %s;""", (limit,))
        sites = [site[0] for site in cursor.fetchall()]
    
    return sites

def updateScrapingDate(site_name: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""UPDATE site
                       SET last_scraped_at = CURRENT_DATE
                       WHERE name = %s;""", (site_name,))
        conn.commit()

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

def wordsPersentage(title_1, title_2):
    chars = [',', '.', ':', ';', '"', '?', '!', '(', ')', '«', '»']
    title_1 = removeChar(title_1, chars)
    title_2 = removeChar(title_2, chars)

    words_1 = set(title_1.split())
    words_2 = set(title_2.split())

    intersection = 2*len(words_1 & words_2)
    total = len(words_1) + len(words_2)

    return intersection / total * 100

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
            return NA


if __name__ == '__main__':
    # urls = getURLs('gameland')
    # print(urls)
    # print(len(urls))
    print(getSitesToScrape())
    print(getSitesToScrape(3))
    updateScrapingDate('gameland')
    print(getSitesToScrape())