import logging
import time
import json

class ScrapingLogger:
    def __init__(self, level=logging.DEBUG, fileName='./scraping.log'):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)
        formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%d-%m-%Y %H:%M:%S')

        fh = logging.FileHandler(fileName)
        fh.setLevel(level=level)
        fh.setFormatter(formater)

        sh = logging.StreamHandler()
        sh.setLevel(level='ERROR')
        sh.setFormatter(formater)

        self.logger.addHandler(fh)
        self.logger.addHandler(sh)

        self.items_scraped = 0

    def startMessage(self):
        self.startTime = time.time()
        self.logger.info('Scraping started')

    def failedURL(self, url, exc_info=False, status_code=None):
        self.logger.error(f'{status_code+' ' if status_code else ''}Failed url: {url}', exc_info=exc_info)

    def failedPagination(self, pageNum = None, status_code=None):
        self.logger.critical(f'{status_code+' ' if status_code else ''}Pagination failed at page {pageNum}')

    def increaseItemsScraped(self, n):
        self.items_scraped += n

    def summarize(self):
        delta_time = time.time() - self.startTime
        summary = json.dumps({
            'runnig_time': '%d:%02d:%02d' % (delta_time//3600, (delta_time%3600)//60, (delta_time%3600)%60),
            'items_scraped': self.items_scraped
            },
            indent=4)

        self.logger.info('Dumping summary\n' +summary+'\n')

if __name__ == '__main__':
    pass
        