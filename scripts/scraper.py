from playwright.sync_api import sync_playwright, Playwright
from playwright_stealth import Stealth

import requests
from bs4 import BeautifulSoup

from dataclasses import dataclass
import time
from functools import partial
from concurrent.futures import ThreadPoolExecutor

from site_classes import *
from proxy import Proxy
from utils import errorCatcher, saveTo

@dataclass 
class Context:
    workers: int = 1
    sleep_break: float = 0


class Scraper:
    def __init__(self, siteClass: Gameland, proxies=Proxy(), context: Context = Context()):
        self.site = siteClass()
        self.logger = self.site.logger
        self.proxies = proxies
        self.session = requests.Session()
        self.pathToSave = f'./data/{self.site.siteName}_data.csv'
        self.context = context

        # Making functions that apply on html-elements
        # return None if error occurs
        self.formaters = {k: partial(errorCatcher, v, lambda _: None)
                          for k, v in self.site.dataFormaters.items()}
        
        self.failedURLs = []

    def scrapeDetailPage(self, url: str) -> dict:
        try:
            page = self.session.get(url, proxies=self.proxies.proxyForRequests())
        except:
            self.logger.failedURL(url, exc_info=True)
            return url
        if page.status_code != 200:
             self.logger.failedURL(url, status_code=page.status_code)

        soup = BeautifulSoup(page.text, 'html.parser')

        # Selecting html-elements with data
        dataContainers = {k: soup.select_one(selector) for k, selector in self.site.dataSelectors.items()}
        # Extracting data from them
        data = {k:form(dataContainers[k]) for k, form in self.formaters.items()}
        data.update({'url': url})

        # Sleep for some time to reduce load no server
        time.sleep(self.context.sleep_break)

        return data
        
    def scrapeWihtThreads(self, links: list, workers: int):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            result = list(executor.map(self.scrapeDetailPage, links))

        return result
    
    def scrapeCatalog(self, soup: BeautifulSoup, workers: int = 1):
        links = self._getLinks(soup)
        data = self.scrapeWihtThreads(links, workers=workers)

        self.failedURLs.extend([item for item in data if type(item) is str])
        gamesData = [item for item in data if type(item) is dict]

        return gamesData
    
    def scrape(self, pathToSave = '', stopAt: int=None):
        pathToSave = self.pathToSave if pathToSave == '' else pathToSave
        self.logger.startMessage()
        self._updateSession()
        nextPageLink = self.site.startUrl
        # Saving
        columnNames = list(self.site.dataSelectors.keys()) + ['url']
        saveTo(pathToSave, [], mode='newfile', columns=columnNames)

        while nextPageLink is not None:
            try:
                page = self.session.get(nextPageLink, proxies=self.proxies.proxyForRequests())
            except:
                self.logger.failedPagination(nextPageLink, exc_info=True)
            if page.status_code != 200:
                self.logger.failedPagination(nextPageLink, status_code=page.status_code)

            soup = BeautifulSoup(page.text, 'html.parser')
            gamesData = self.scrapeCatalog(soup, workers=self.context.workers)
            self.logger.increaseItemsScraped(len(gamesData))
            saveTo(pathToSave, gamesData)

            pageNum = soup.select_one('span[class*="pager__item is-active"]').text.strip()
            print(pageNum, self.logger.items_scraped)

            nextPageLink = errorCatcher(lambda _:self.site.baseUrl + soup.select_one(self.site.nextPageSelector).get('href'),
                                        lambda _: None, None)
            if stopAt is not None:
                if stopAt <= self.logger.items_scraped: break
        self.logger.summarize()
        return self.failedURLs
            
    
    def _getLinks(self, soup: BeautifulSoup):
        elements = soup.select(self.site.linksSelector)
        links = [self.site.baseUrl + el.get('href') for el in elements]
        return links

    def _getHeaders(self):
        with Stealth().use_sync(sync_playwright()) as playwright:
            browser = playwright.chromium.launch(headless=False, proxy=self.proxies.proxyForPlaywright())
            page = browser.new_page()
            page.goto(self.site.startUrl)
            with page.expect_response(self.site.startUrl, timeout=30000) as response_info:
                    response = response_info.value
            request = response.request
            browser.close()

        return request.headers

    def _updateSession(self):
        headers = self._getHeaders()
        self.session.headers.update(headers)

        

if __name__ == '__main__':
     proxy = Proxy(r'C:\Users\User\Jupyter Folder\Webshare 10 proxies.txt')
     context = Context(7, 4)
     scr = Scraper(Gameland, proxy, context)
     data = scr.scrape()

     print(data)
