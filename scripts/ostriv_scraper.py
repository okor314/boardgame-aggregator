import requests
from bs4 import BeautifulSoup
import pandas as pd

def errorCatcher(func, heandler, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        return heandler(e)
def scrapePage(soup, dataDictionary):
    gamePreviews = soup.find_all('li', attrs={'data-qaid': 'product-block'})
    ids = [errorCatcher(lambda x: x.text, lambda x: None, game.find('span', attrs={'title': 'Код:'})) for game in gamePreviews]
    names = [errorCatcher(lambda x: x.text, lambda x: None, game.find('a', attrs={'class': 'b-product-gallery__title'})) for game in gamePreviews]
    prices = [errorCatcher(lambda x: float(x.text.replace('\xa0', '')[:-2].replace(',', '.')), lambda x: None, game.find('span', attrs={'class': 'b-product-gallery__current-price'}))
              for game in gamePreviews]
    in_stock_flags = [errorCatcher(lambda x: x.text.lower() in ('готово до відправки', 'в наявності'), lambda x: None, 
                                   game.find('span', attrs={'data-qaid': 'presence_data'})) for game in gamePreviews]
    links = [errorCatcher(lambda x:'https://gameisland.prom.ua/' + x.get('href'), lambda x: None, game.find('a', attrs={'class': 'b-product-gallery__title'}))
             for game in gamePreviews]

    dataDictionary['id'].extend(ids)
    dataDictionary['name'].extend(names)
    dataDictionary['price'].extend(prices)
    dataDictionary['in_stock'].extend(in_stock_flags)
    dataDictionary['link'].extend(links)


def scrapeOstriv(dataDictionary):
    mainPageURL = 'https://gameisland.prom.ua/ua/g113090916-nastolnye-igry'
    pageNum = 1
    
    page = requests.get(mainPageURL)
    if page.status_code != 200: return
    soup = BeautifulSoup(page.text, 'html.parser')

    #Scraping items
    scrapePage(soup, dataDictionary)

    pageNum += 1
    nextPageLink = f'https://gameisland.prom.ua/ua/g113090916-nastolnye-igry/page_{pageNum}'

    while pageNum <= 39:
        newPage = requests.get(nextPageLink)
        if newPage.status_code != 200:
            print(f'Requests error {newPage.status_code} on page {pageNum}')
            pageNum += 1
            nextPageLink = f'https://gameisland.prom.ua/ua/g113090916-nastolnye-igry/page_{pageNum}'
            continue
        print(pageNum)
        newSoup = BeautifulSoup(newPage.text, 'html.parser')
        
        scrapePage(newSoup, dataDictionary)
    
        pageNum += 1
        nextPageLink = f'https://gameisland.prom.ua/ua/g113090916-nastolnye-igry/page_{pageNum}'
        
if __name__ == '__main__':
  data = {
    'id': [],
    'name': [],
    'price': [],
    'in_stock': [],
    'link': []
  }

  scrapeOstriv(data)
