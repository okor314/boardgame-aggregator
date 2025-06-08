import requests
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor
import time

def errorCatcher(func, heandler, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        heandler(e)

def scrapingWithThreads(links):
  
    with ThreadPoolExecutor(max_workers=10) as executor:
        result = executor.map(isGameIiStock, links)

    in_stock_flags = {url: flag for (url, flag) in result}
    return [in_stock_flags[url] for url in links]  

def scrapeGames(pageSoup, dataDictionary):
    gamePreviews = pageSoup.find_all('li', attrs={'class': 'catalog-grid__item'})
    ids = [errorCatcher(lambda x: x.text.strip().split(' ')[-1], lambda x: None, game.find('div', attrs={'class': 'catalogCard-code'})) for game in gamePreviews]
    names = [errorCatcher(lambda x: x.find('a')['title'], lambda x: None, game.find('div', attrs={'class': 'catalogCard-title'})) for game in gamePreviews]
    prices = [errorCatcher(lambda x: float(x.text.strip().replace(' ', '')[:-3]), lambda x: None, game.find('div', attrs={'class': 'catalogCard-price'})) for game in gamePreviews]
    #in_stock_flags = []
    links = [errorCatcher(lambda x: 'https://gameland.com.ua' + x.find('a')['href'], lambda x: None,
                          game.find('div', attrs={'class': 'catalogCard-title'})) for game in gamePreviews]

    # Checking if products available in stock
    in_stock_flags = scrapingWithThreads(links)
        
    dataDictionary['id'].extend(ids)
    dataDictionary['name'].extend(names)
    dataDictionary['price'].extend(prices)
    dataDictionary['in_stock'].extend(in_stock_flags)
    dataDictionary['link'].extend(links)


def scrapeGameland(dataDictionary, linkSraper):
    mainPageURL = 'https://gameland.com.ua/catalog/'

    page = requests.get(mainPageURL)
    if page.status_code != 200: return
    soup = BeautifulSoup(page.text, 'html.parser')

    scrapeGames(soup, dataDictionary, linkSraper)

    nextPageLink = soup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})

    while nextPageLink is not None:
        newPageLink = 'https://gameland.com.ua' + nextPageLink['href']
        newPage = requests.get(newPageLink)
        if newPage.status_code != 200:
            print(f'Break on page {newPageLink.split('page=')[1][0]}')
            return
            
        newSoup = BeautifulSoup(newPage.text, 'html.parser')
        
        scrapeGames(newSoup, dataDictionary, linkSraper)
        
        nextPageLink = newSoup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})





if __name__ == '__main__':
    data = {
    'id': [],
    'name': [],
    'price': [],
    'in_stock': [],
    'link': []
    }

    print('0')

    start = time.time()
    scrapeGameland(data)
    end = time.time()
    print(end - start)

    df = pd.DataFrame(data)
    print(df)
