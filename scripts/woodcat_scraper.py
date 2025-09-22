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
    
def getGameData(url: str, proxy: Proxy, pause: float = 0):
    page = requests.get(url, proxies=proxy.proxyForRequests(5))
    if page.status_code != 200:
        return None
    soup = BeautifulSoup(page.text, 'html.parser')

    id = errorCatcher(lambda _: soup.find('div', attrs={'class': 'product-header__code product-header__code--filled'}).text.strip().replace('Артикул: ', ''),
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


def scrapeGames(pageSoup, dataDictionary):
    links = ['https://woodcat.com.ua' + game.find('a').get('href') for game in pageSoup.find_all('div', attrs={'class':'catalogCard-view'})]
    
    for link in links:
        gamePage = requests.get(link)
        if gamePage.status_code != 200:
            print(f'Request error: {gamePage.status_code}', link)
        else:
            newSoup = BeautifulSoup(gamePage.text, 'html.parser')
            
            ID = newSoup.find('div', attrs={'class': 'product-header__code product-header__code--filled'}).text.split('\n')[2].strip()
            name = ' '.join(newSoup.find('h1', attrs={'class': 'product-title'}).text.split(' ')[2:])
            price = float(newSoup.find('meta', attrs={'itemprop': 'price'}).get('content'))
            try:
                genre = [row.find('td').text.strip() for row in newSoup.find_all('tr', attrs={'class': 'product-features__row'})
                        if 'Жанр' in row.find('th').text][0]
            except:
                genre = None
            in_stock = 'в наявності' == newSoup.find('div', attrs={'class': 'product-header__availability'}).text.strip().lower()

            dataDictionary['id'].append(ID)
            dataDictionary['name'].append(name)
            dataDictionary['price'].append(price)
            dataDictionary['genre'].append(genre)
            dataDictionary['in_stock'].append(in_stock)
            dataDictionary['link'].append(link)

def scrapeWoodcat(dataDictionary):
    # Get first page with board games
    mainPageURL = 'https://woodcat.com.ua/katalog/1054/?gad_source=1&gad_campaignid=21798011267&gbraid=0AAAAA9yheJDxTz6Svxd2Qtd3-13ivOh_i&gclid=Cj0KCQjw0LDBBhCnARIsAMpYlAoD_u36hVrC7aMKQc_topo48uZyPB4kkUW45Y6bPN6ITzrHqmjHlZEaAt_nEALw_wcB'

    page = requests.get(mainPageURL)
    if page.status_code != 200: return
    soup = BeautifulSoup(page.text, 'html.parser')

    scrapeGames(soup, dataDictionary)

    nextPageLink = soup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})

    while nextPageLink is not None:
        newPageLink = 'https://woodcat.com.ua' + nextPageLink['href']
        newPage = requests.get(newPageLink)
        if newPage.status_code != 200:
            print(f'Break on page {newPageLink.split('page=')[1][0]}')
            return
            
        newSoup = BeautifulSoup(newPage.text, 'html.parser')
        scrapeGames(newSoup, dataDictionary)
        nextPageLink = newSoup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})

if __name__ == '__main__':
  data = {
    'id': [],
    'name': [],
    'price': [],
    'genre': [],
    'in_stock': [],
    'link': []
  }
  
  scrapeWoodcat(data)
        
