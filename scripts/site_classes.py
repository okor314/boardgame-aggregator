from logger import ScrapingLogger

class HoroshoSite:
    siteName = 'horosho'

    def __init__(self):
        self._baseUrl = 'https://www.google.com'
        self.startUrl = 'https://www.google.com/'
        self.logger = ScrapingLogger(self.siteName)

        self._dataSelectors = {
            'id':       'div.product-header__code',
            'title':    'h1.product-title',
            'in_stock': 'div.product-header__availability',
            'price':    'meta[itemprop="price"]',
            'players':  'tr.product-features__row:has(th:contains("Кількість гравців")) td',
            'age':      'tr.product-features__row:has(th:contains("Вік")) td',
            'maker':    'tr.product-features__row:has(th:contains("Видавець")) td',
            'bbg_url':  'tr.product-features__row:has(th:contains("Рейтинг")) td a'
        }

        # Functions to use on selected hmtl-elements
        self._dataFormaters = {
            'id':       lambda x: x.text.strip().replace('Артикул: ', ''),
            'title':    lambda x: x.text.strip().replace('Настільна гра ', ''),
            'in_stock': lambda x: x.text.strip(),
            'price':    lambda x: float(x.get('content')),
            'players':  lambda x: x.text.strip(),
            'age':      lambda x: x.text.strip(),
            'maker':    lambda x: x.text.strip(),
            'bbg_url':  lambda x: x.get('href')
        }

        self.linksSelector = 'div.catalogCard-title a'
        self.nextPageSelector = 'a[class="pager__item pager__item--forth j-catalog-pagination-btn"]'

    @property
    def baseUrl(self):
        return self._baseUrl
    
    @property
    def dataSelectors(self):
        return self._dataSelectors
    
    @property
    def dataFormaters(self):
        return self._dataFormaters
    
class Gameland(HoroshoSite):
    siteName = 'gameland'

    def __init__(self):
        super().__init__()
        self._baseUrl = 'https://gameland.com.ua'
        self.startUrl = 'https://gameland.com.ua/catalog/'

class Geekach(HoroshoSite):
    siteName = 'geekach'

    def __init__(self):
        super().__init__()
        self._baseUrl = 'https://geekach.com.ua'
        self.startUrl = 'https://geekach.com.ua/nastilni-ihry/'

        self._dataSelectors.update(
            {
                'id':       'meta[itemprop="sku"]',
                'players':  'tr.product-features__row:has(th:contains("Гравців")) td',
                'bbg_url':  'tr.product-features__row:has(th:contains("BBG")) td'
            }
        )
        self._dataFormaters.update(
            {
                'id':       lambda x: x.get('content'),
            }
        )

class Woodcat(HoroshoSite):
    siteName = 'woodcat'

    def __init__(self):
        super().__init__()
        self._baseUrl = 'https://woodcat.com.ua'
        self.startUrl = 'https://woodcat.com.ua/katalog/1054/'

        self._dataSelectors.update(
            {
                'id': 'meta[itemprop="sku"]'
            }
        )
        self._dataFormaters.update(
            {
                'id': lambda x: x.get('content'),
            }
        )
     
        del self._dataSelectors['bbg_url']
        del self._dataFormaters['bbg_url']

if __name__ == '__main__':
    instance = Gameland()
    c = list(instance.dataSelectors.keys()) + ['url']
    print(c)
    