import requests
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor
import time

from proxy import Proxy

def errorCatcher(func, heandler, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        heandler(e)

def scrapingWithThreads(links: list, workers: int, proxy: Proxy):
    proxies = [proxy for _ in links]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        result = executor.map(getGameData, links, proxies)

    return result

def getGameData(url: str, proxy: Proxy):
    page = requests.get(url, proxies=proxy.proxyForRequests(5))
    if page.status_code != 200:
        return None
    soup = BeautifulSoup(page.text, 'html.parser')

    id = errorCatcher(lambda _: soup.find('div', attrs={'class': 'product-header__code'}).text.strip(),
                      lambda _: None, None)
    title = soup.find('h1', attrs={'class': 'product-title'}).text.strip().replace('Настільна гра ', '')
    in_stock = errorCatcher(lambda _: soup.find('div', attrs={'class': 'product-header__availability'}).text.strip(),
                      lambda _: None, None)
    price = errorCatcher(lambda _: float(soup.find('meta', attrs={'itemprop': 'price'}).get('content')),
                         lambda _: None, None)
    
    # Extracting data from the features table
    table = soup.find('table', attrs={'class': 'product-features__table'})
    rows = table.find_all('tr')
    players = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Кількість гравців' in row.text][0],
                           lambda _: None, None)
    age = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Вік' in row.text][0],
                       lambda _: None, None)
    maker = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Видавець' in row.text][0],
                         lambda _: None, None)
    bbg_url = errorCatcher(lambda _: [row.find('td').find('a').get('href') for row in rows if 'Рейтинг' in row.text][0],
                           lambda _: None, None)
    
    # Structuring data to dict
    data = {
        'id': id,
        'title': title,
        'in_stock': in_stock,
        'price': price,
        'players': players,
        'age': age,
        'maker': maker,
        'bbg_url': bbg_url
    }
    
    return data

    


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
    # data = {
    # 'id': [],
    # 'name': [],
    # 'price': [],
    # 'in_stock': [],
    # 'link': []
    # }

    # print('0')

    # start = time.time()
    # scrapeGameland(data)
    # end = time.time()
    # print(end - start)

    # df = pd.DataFrame(data)
    # print(df)

    proxy = Proxy(r'C:\Users\User\Jupyter Folder\Webshare 10 proxies.txt')
    d = getGameData('https://gameland.com.ua/bytva-restoraniv-rival-restaurants/', proxy)
    print(d)
