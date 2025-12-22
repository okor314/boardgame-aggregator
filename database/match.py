from dataclasses import dataclass
from collections import defaultdict
from typing import List, Dict, Set

from database.utils import extract_english_fragments, extract_ukrainian_fragments
from database.site_database import removeChar
from database.utils import compareTitles, wordsPersentage

FUZZ_BARRIER = 60

@dataclass(frozen=True)
class NormalizedTitle:
    raw: str
    eng: frozenset[str]
    ukr: frozenset[str]
    words: frozenset[str]

def normalizeTitle(title: str) -> NormalizedTitle:
    chars = [',', '.', ':', ';', '"', '?', '!', '(', ')', '«', '»', '/']
    cleaned = removeChar(title.lower(), chars)
    
    return NormalizedTitle(
        raw = title,
        eng = frozenset(extract_english_fragments(cleaned).split()),
        ukr = frozenset(extract_ukrainian_fragments(cleaned).split()),
        words = frozenset(cleaned.split())
    )

def indexWords(games: Dict[int, NormalizedTitle]) -> Dict[str, List[int]]:
    """Function create dictionary that maps word to games
    with title which contains that word."""
    index = defaultdict(set)

    for game_id, norm in games.items():
        for word in norm.words:
            if len(word) > 2:
                index[word].add(game_id)

    return index

def normWordsPercentage(words_1: set, words_2: set):
    if not words_1 or not words_2:
        return 0
    
    intersection = 2 * len(words_1 & words_2)
    total = len(words_1) + len(words_2)

    return intersection / total * 100

def compareNormTitles(title_1: NormalizedTitle, title_2: NormalizedTitle):
    eng_score = normWordsPercentage(title_1.eng, title_2.eng)
    ukr_score = normWordsPercentage(title_1.ukr, title_2.ukr)

    if eng_score and ukr_score:
        return (eng_score + ukr_score) / 2
    
    return normWordsPercentage(title_1.words, title_2.words)

def bestMatch(title: NormalizedTitle, choises: Dict[int, NormalizedTitle], minScore = FUZZ_BARRIER):
    best = None
    bestScore = minScore

    for game_id, choiseTitle in choises.items():
        score = compareNormTitles(title, choiseTitle)
        if score > bestScore:
            best = (game_id, title.raw, score)
            bestScore = score

        if score == 100:
            break

    return best

def getCandidates(title: NormalizedTitle, games: Dict[int, NormalizedTitle], word_index: Dict[str, Set[int]]):
    """Return dictionary with games which have at least one common
    word with `title`"""
    candidate_ids = set()
    for word in title.words:
        candidate_ids |= word_index[word]

    return {id: games[id] for id in candidate_ids}

def findMatch(title: NormalizedTitle, games: Dict[int, NormalizedTitle], word_index: Dict[str, Set[int]]):
    candidates = getCandidates(title, games, word_index)
    if not candidates:  # Use all games if there no candidates
        candidates = games

    return bestMatch(title, candidates)

def removeGame(game_id: int, games: Dict[int, NormalizedTitle], word_index: Dict[str, Set[int]]):
    title = games[game_id]
    del games[game_id]

    for word in title.words:
        ids = word_index.get(word)
        if ids:
            word_index[word].discard(game_id)

if __name__ == '__main__':
    games = {1: normalizeTitle('Saboteur 2 (Саботер 2)'), 2: normalizeTitle('IQ Коханці IQ Lovers'),
             3: normalizeTitle('Саботер'),}
    word_index = indexWords(games)

    m = findMatch(games[3], games, word_index)
    print(m)
    