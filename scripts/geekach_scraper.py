import requests
from bs4 import BeautifulSoup
import pandas as pd

def errorCatcher(func, heandler, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        return heandler(e)

def scrapeGames(pageSoup, dataDictionary):
    gamePreviews = pageSoup.find_all('li', attrs={'class': 'catalog-grid__item'})
    ids = [errorCatcher(lambda x: x.text.strip().split(' ')[-1], lambda x: None, game.find('div', attrs={'class': 'catalogCard-code'})) for game in gamePreviews]
    names = [errorCatcher(lambda x: x.find('a')['title'], lambda x: None, game.find('div', attrs={'class': 'catalogCard-title'})) for game in gamePreviews]
    prices = [errorCatcher(lambda x: float(x.text.strip().replace(' ', '')[:-3]), lambda x: None, game.find('div', attrs={'class': 'catalogCard-price'})) for game in gamePreviews]
    in_stock_flags = [errorCatcher(lambda x: 'в наявності' == x.text.strip().lower(), lambda x: None, game.find('div', attrs={'class': 'catalogCard-availability'})) for game in gamePreviews]
    links = [errorCatcher(lambda x: 'https://geekach.com.ua' + x.find('a')['href'], lambda x: None, game.find('div', attrs={'class': 'catalogCard-title'})) for game in gamePreviews]

    dataDictionary['id'].extend(ids)
    dataDictionary['name'].extend(names)
    dataDictionary['price'].extend(prices)
    dataDictionary['in_stock'].extend(in_stock_flags)
    dataDictionary['link'].extend(links)

def scrapeGeekach(dataDictionary):
    # Get first page with board games
    mainPageURL = 'https://geekach.com.ua/nastilni-ihry/'
    
    page = requests.get(mainPageURL)
    if page.status_code != 200: return
    soup = BeautifulSoup(page.text, 'html.parser')

    scrapeGames(soup, dataDictionary)
    nextPageLink = soup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})

    # Go throught all pages
    while nextPageLink is not None:
        newPageLink = 'https://geekach.com.ua' + nextPageLink['href']
        newPage = requests.get(newPageLink)
        if newPage.status_code != 200:
            print(f'Break on page {newPageLink.split('page=')[1][0]}')
            return
        print(newPageLink.split('page=')[1][0])
        newSoup = BeautifulSoup(newPage.text, 'html.parser')
        scrapeGames(newSoup, dataDictionary)
        
        nextPageLink = newSoup.find('a', attrs={'class': 'pager__item pager__item--forth j-catalog-pagination-btn'})
        

if __name__ == '__main__':
  data = {
    'id': [],
    'name': [],
    'price': [],
    'in_stock': [],
    'link': []
  }
  scrapeGeekach(data)
