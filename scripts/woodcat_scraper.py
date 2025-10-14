import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time

from proxy import Proxy
from utils import errorCatcher, saveTo
    
def scrapingWithThreads(links: list, workers: int, proxy: Proxy, pause: float = 0):
    proxies = [proxy for _ in links]
    pauses = [pause for _ in links]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        result = executor.map(getGameData, links, proxies, pauses)

    return result
    
def getGameData(url: str, proxy: Proxy, pause: float = 0):
    try:
        page = requests.get(url, proxies=proxy.proxyForRequests())
    except:
        print(f'Failed: {url}')
        return url
    if page.status_code != 200:
        return url
    soup = BeautifulSoup(page.text, 'html.parser')

    id = errorCatcher(lambda _: soup.find('div', attrs={'class': 'product-header__code product-header__code--filled'}).text.split('\n')[2].strip(),
                      lambda _: None, None)
    title = soup.find('h1', attrs={'class': 'product-title'}).text.strip().replace('Настільна гра ', '')
    in_stock = errorCatcher(lambda _: soup.find('div', attrs={'class': 'product-header__availability'}).text.strip(),
                      lambda _: None, None)
    price = errorCatcher(lambda _: float(soup.find('meta', attrs={'itemprop': 'price'}).get('content')),
                         lambda _: None, None)
    
    # Extracting data from the features table
    table = soup.find('table', attrs={'class': 'product-features__table'})
    rows = errorCatcher(lambda _: table.find_all('tr'),
                           lambda _: None, None)
    players = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Кількість гравців' in row.text][0],
                           lambda _: None, None)
    age = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Вік' in row.find('th').text][0],
                       lambda _: None, None)
    maker = errorCatcher(lambda _: [row.find('td').text.strip() for row in rows if 'Видавець' in row.text][0],
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
        'url': url
    }

    # Pause before making next request
    time.sleep(pause)
    
    return data


def getLinks(pageSoup: BeautifulSoup) -> list:
    links = ['https://woodcat.com.ua' + game.find('a').get('href')
              for game in pageSoup.find_all('div', attrs={'class':'catalogCard-view'})]
    
    return links

def scrapeWoodcat(proxy: Proxy = Proxy(), workers: int = 1, pause: float = 0,
                  stopAt: int = None, pathToSave = './data/woodcat_data.csv') -> list:
    # Get first page with board games
    mainPageURL = 'https://woodcat.com.ua/katalog/1054/?gad_source=1&gad_campaignid=21798011267&gbraid=0AAAAA9yheJDxTz6Svxd2Qtd3-13ivOh_i&gclid=Cj0KCQjw0LDBBhCnARIsAMpYlAoD_u36hVrC7aMKQc_topo48uZyPB4kkUW45Y6bPN6ITzrHqmjHlZEaAt_nEALw_wcB'
    resultData = []
    failedURLs = []

    page = requests.get(mainPageURL, proxies=proxy.proxyForRequests())
    if page.status_code != 200: return
    soup = BeautifulSoup(page.text, 'html.parser')

    # Scraping data
    links = getLinks(soup)
    gamesData = scrapingWithThreads(links, workers=workers, proxy=proxy, pause=pause)
    # Removing failed urls
    gamesData = [item for item in gamesData if type(item) is dict]
    failedURLs.extend([item for item in gamesData if type(item) is str])
    # Saving
    saveTo(pathToSave, gamesData, mode='newfile')
    resultData.extend(gamesData)

    nextPageLink = soup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})

    while nextPageLink is not None:
        if stopAt is not None:
            if len(resultData) >= stopAt:
                break

        newPageLink = 'https://woodcat.com.ua' + nextPageLink['href']
        newPage = requests.get(newPageLink, proxies=proxy.proxyForRequests())
        if newPage.status_code != 200:
            print(f'Break on page {newPageLink.split('page=')[1][0]}')
            return
            
        newSoup = BeautifulSoup(newPage.text, 'html.parser')

        links = getLinks(soup)
        gamesData = scrapingWithThreads(links, workers=workers, proxy=proxy, pause=pause)
        # Removing failed urls
        gamesData = [item for item in gamesData if type(item) is dict]
        failedURLs.extend([item for item in gamesData if type(item) is str])
        # Saving
        saveTo(pathToSave, gamesData)
        resultData.extend(gamesData)

        print(f'Page {newPageLink.split('page=')[1].split('/')[0]}, items {len(resultData)}')

        nextPageLink = newSoup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})
    
    # Trying scrape failed urls one more time
    gamesData = scrapingWithThreads(failedURLs, workers=workers, proxy=proxy, pause=pause)
    # Removing failed urls
    gamesData = [item for item in gamesData if type(item) is dict]
    # Saving
    saveTo(pathToSave, gamesData)
    resultData.extend(gamesData)

    
    return resultData

if __name__ == '__main__':
#   data = {
#     'id': [],
#     'name': [],
#     'price': [],
#     'genre': [],
#     'in_stock': [],
#     'link': []
#   }
  
#   scrapeWoodcat(data)
    proxy = Proxy(r'C:\Users\User\Jupyter Folder\Webshare 10 proxies.txt')
    data = scrapeWoodcat(proxy=proxy, workers=7, pause=3)
    print(len(data))
