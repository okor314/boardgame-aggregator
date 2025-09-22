import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time

from proxy import Proxy

def errorCatcher(func, heandler, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        return heandler(e)
    
def scrapingWithThreads(links: list, workers: int, proxy: Proxy, pause: float = 0):
    proxies = [proxy for _ in links]
    pauses = [pause for _ in links]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        result = executor.map(getGameData, links, proxies, pauses)

    return result

def getGameData(url: str, proxy: Proxy, pause: float = 0):
    page = requests.get(url, proxies=proxy.proxyForRequests(5))
    if page.status_code != 200:
        return None
    soup = BeautifulSoup(page.text, 'html.parser')

    title = soup.find('h1', attrs={'class': 'product-title'}).text.strip().replace('Настільна гра ', '')
    in_stock = errorCatcher(lambda _: soup.find('div', attrs={'class': 'product-header__availability'}).text.strip(),
                      lambda _: None, None)
    price = errorCatcher(lambda _: float(soup.find('meta', attrs={'itemprop': 'price'}).get('content')),
                         lambda _: None, None)
    
    # Extracting data from the features table
    table = soup.find('table', attrs={'class': 'product-features__table'})
    rows = table.find_all('tr')
    players = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Гравців' in row.text][0],
                           lambda _: None, None)
    age = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Вік' in row.text][0],
                       lambda _: None, None)
    maker = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Видавець' in row.text][0],
                         lambda _: None, None)
    bbg_url = errorCatcher(lambda _: [row.find('td').find('a').get('href') for row in rows if 'BGG' in row.text][0],
                           lambda _: None, None)
    id = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Артикул' in row.text][0],
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

    # Pause before making next request
    time.sleep(pause)
    
    return data

def getLinks(pageSoup: BeautifulSoup) -> list:
    gamePreviews = pageSoup.find_all('li', attrs={'class': 'catalog-grid__item'})
    links = [errorCatcher(lambda x: 'https://geekach.com.ua' + x.find('a')['href'],
                          lambda x: None, game.find('div', attrs={'class': 'catalogCard-title'})) for game in gamePreviews]
    
    return links


def scrapeGeekach(proxy: Proxy, workers: int = 1, pause: float = 0, stopAt: int = None) -> list:
    # Get first page with board games
    mainPageURL = 'https://geekach.com.ua/nastilni-ihry/'
    resultData = []
    
    page = requests.get(mainPageURL, proxies=proxy.proxyForRequests(5))
    if page.status_code != 200: return
    soup = BeautifulSoup(page.text, 'html.parser')

    # Scraping data
    links = getLinks(soup)
    resultData.extend(scrapingWithThreads(links, workers=workers, proxy=proxy, pause=pause))

    nextPageLink = soup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})

    # Go throught all pages
    while nextPageLink is not None:
        # Checking if number of games reach desired value
        if stopAt is not None:
            if len(resultData) >= stopAt:
                break
        print(len(resultData))

        newPageLink = 'https://geekach.com.ua' + nextPageLink['href']
        newPage = requests.get(newPageLink, proxy=proxy.proxyForRequests(5))
        if newPage.status_code != 200:
            print(f'Break on page {newPageLink.split('page=')[1][0]}')
            return resultData
        print(newPageLink.split('page=')[1][0])

        newSoup = BeautifulSoup(newPage.text, 'html.parser')
        links = getLinks(newSoup)
        resultData.extend(scrapingWithThreads(links, workers=workers, proxy=proxy, pause=pause))
        
        nextPageLink = newSoup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})
    
    return resultData

if __name__ == '__main__':
#   data = {
#     'id': [],
#     'name': [],
#     'price': [],
#     'in_stock': [],
#     'link': []
#   }
#   scrapeGeekach(data)

    proxy = Proxy(r'C:\Users\User\Jupyter Folder\Webshare 10 proxies.txt')
    data = scrapeGeekach(proxy=proxy, workers=3, pause=5, stopAt=20)
    print(data)
    print(len(data))
