from playwright.sync_api import sync_playwright, Playwright
from playwright_stealth import Stealth

import requests
from bs4 import BeautifulSoup

from dataclasses import dataclass
import time
from functools import partial
from concurrent.futures import ThreadPoolExecutor

from scripts.site_classes import *
from scripts.proxy import Proxy
from scripts.utils import errorCatcher, TableWriter
from database.utils import getURLs

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
        self.context = context
        self.writer = TableWriter(self.site.fieldnames, output_dir='./data')

        # Making functions that apply on html-elements
        # return None if error occurs
        self.formaters = {k: partial(errorCatcher, v, lambda _: None)
                          for k, v in self.site.dataFormaters.items()}
        
        self.failedURLs = []

        # URLs that already in database
        self.existingURLs = getURLs(self.site.siteName)

    def scrapeDetailPage(self, url: str) -> dict:
        try:
            page = self.session.get(url, proxies=self.proxies.proxyForRequests())
        except:
            self.logger.failedURL(url, exc_info=True)
            self.failedURLs.append(url)
            return {'url': url, 'failed': True}
        if page.status_code != 200:
             self.logger.failedURL(url, status_code=page.status_code)

        soup = BeautifulSoup(page.text, 'html.parser')

        # Selecting html-elements with data
        dataContainers = {k: soup.select_one(selector) for k, selector in self.site.dataSelectors.items()}
        # Extracting data from them
        data = {k:form(dataContainers[k]) for k, form in self.formaters.items()}

        # Sleep for some time to reduce load no server
        time.sleep(self.context.sleep_break)

        return data
        
    def scrapeWihtThreads(self, links: list, workers: int):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            result = list(executor.map(self.scrapeDetailPage, links))

        return result
    
    def scrapeCatalog(self, soup: BeautifulSoup, workers: int = 1):
        catalogData = self.site.extractCatalogData(soup.__repr__())

        links = self._getLinks(catalogData)
        detailData = self.scrapeWihtThreads(links, workers=workers)

        gamesData = self._mergeData(detailData, catalogData)

        return gamesData
    
    def scrape(self, tableName: str, stopAt: int=None):
        self.logger.startMessage()
        self._updateSession()
        nextPageLink = self.site.startUrl

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
            # Saving
            self.writer.writerows(gamesData, tableName)

            pageNum = soup.select_one('span[class*="pager__item is-active"]').text.strip()
            print(pageNum, self.logger.items_scraped)

            nextPageLink = errorCatcher(lambda _:self.site.baseUrl + soup.select_one(self.site.nextPageSelector).get('href'),
                                        lambda _: None, None)
            if stopAt is not None:
                if stopAt <= self.logger.items_scraped: break
        self.logger.summarize()
        return self.failedURLs
            
    
    def _getLinks(self, catalogData: list[dict]):
        # URLs that have missing data in catalog
        links = [product.get('url') for product in catalogData 
                 if (not product.get('price')) or (product.get('in_stock') is None)]
        # URLs of new products
        newLinks = [product.get('url') for product in catalogData 
                    if product.get('url') not in self.existingURLs]
        links.extend(newLinks)
        return set(links)

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

    def _mergeData(self, detailData: list[dict], shortData: list[dict]) -> list[dict]:
        """Helper function to merge data from catalog page and data from detail pages."""
        failedURLs = [product.get('url') for product in detailData if product.get('failed')]
        right = {product.get('url'): product for product in detailData
                 if product.get('url') not in failedURLs}
        left = {product.get('url'): product for product in shortData
                if product.get('url') not in failedURLs}

        # Replacing with detailed data
        left.update(right)
        return list(left.values())
        

if __name__ == '__main__':
    proxy = Proxy(r'C:\Users\User\Jupyter Folder\Webshare 10 proxies.txt')
    context = Context(7, 4)
    scr = Scraper(Woodcat, proxy, context)

    data = scr.scrape(tableName='woodcat')

    print(data)
