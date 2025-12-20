from scripts.logger import ScrapingLogger
import re
import json
from bs4 import BeautifulSoup
from scripts.utils import errorCatcher

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
            'url':      'link[rel="canonical"]',
            'bgg_id':  'tr.product-features__row:has(th:contains("Рейтинг")) td a'
        }

        # Functions to use on selected hmtl-elements
        self._dataFormaters = {
            'id':       lambda x: x.text.strip().replace('Артикул: ', ''),
            'title':    lambda x: x.text.strip().replace('Настільна гра ', ''),
            'in_stock': lambda x: x.text.strip() == 'В наявності',
            'price':    lambda x: float(x.get('content')),
            'players':  lambda x: x.text.strip(),
            'age':      lambda x: x.text.strip(),
            'maker':    lambda x: x.text.strip(),
            'url':      lambda x: x.get('href'),
            'bgg_id':  lambda x: int(re.findall(r'(\d+)', x.get('href'))[0])
        }
        
        self._fieldnames = list(self._dataSelectors.keys())
        self.linksSelector = 'div.catalogCard-title a'
        self.nextPageSelector = 'link[rel="next"]'

    @property
    def baseUrl(self):
        return self._baseUrl
    
    @property
    def dataSelectors(self):
        return self._dataSelectors
    
    @property
    def dataFormaters(self):
        return self._dataFormaters
    
    @property
    def fieldnames(self):
        return self._fieldnames
    
    @staticmethod
    def extractCatalogData(html):
        match = re.search(r"products = (\[.*?\]),", html, re.S)
        products = json.loads(match.group(1))
        
        return [{
            'id':       product.get('article'),
            'in_stock': product.get('in_stock'),
            'price':    product.get('price'),
            'url':      product.get('url'),
        } for product in products]
    
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
                'bgg_id':  'tr.product-features__row:has(th:contains("BGG")) td a'
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
     
        del self._dataSelectors['bgg_id']
        del self._dataFormaters['bgg_id']
        self._fieldnames = list(self._dataSelectors.keys())

class Ihromag(HoroshoSite):
    siteName = 'ihromag'

    def __init__(self):
        super().__init__()
        self._baseUrl = 'https://desktopgames.com.ua/ua'
        self.startUrl = 'https://desktopgames.com.ua/ua/catalog/boardgames'

        self._dataSelectors = {
            'id':       'div.code_goods_',
            'title':    'div.view h1',
            'in_stock': 'div.part_block:has(link[itemprop="availability"])',
            'price':    'meta[property="product:price:amount"]',
            'players':  'span:has(img[alt="кількість гравців"])',
            'age':      'span:has(img[alt="вік"])',
            'maker':    'p:is(span[style="display: inline-block"], :contains("Видавець"), :contains("Видавництво"))',
            'url':      'link[rel="canonical"]',
            'bgg_id':   'script[async]:contains("bgg_id")'
        }

        # Functions to use on selected hmtl-elements
        self._dataFormaters = {
            'id':       lambda x: x.text.strip().replace('код: ', ''),
            'title':    lambda x: x.text.strip().replace('Настільна гра ', '').replace(' (UA)', '').replace('(EN)', '(англ.)'),
            'in_stock': lambda x: 'в наявності' in x.text.strip().lower(),
            'price':    lambda x: float(x.get('content')),
            'players':  lambda x: x.text.strip(),
            'age':      lambda x: x.text.strip(),
            'maker':    lambda x: re.findall(r'<b>(?:Видавництво|Видавець):<\/b> ([А-Яа-яІіЇїЄєҐґ\w ]+)\s?(?:\(|<br\/>|<br>)', x.__repr__())[0],
            'url':      lambda x: x.get('href'),
            'bgg_id':   lambda x: int(m) if (m := re.findall(r"bgg_id: '(\d+)'", x.text)[0]) != '0' else None
        }
        
        self._fieldnames = list(self._dataSelectors.keys())
        
        self.nextPageSelector = 'link[rel="next"]'

    def extractCatalogData(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        products = soup.select('li.product')
        
        return [{
            'id':       product.select_one('div.over_goods').get('data-id'),
            'in_stock': not bool(product.select_one('span.not_sale')),
            'price':    product.select_one('div.over_goods').get('data-price'),
            'url':      self.baseUrl + product.select_one('meta[itemprop="url"]').get('content'),
        } for product in products 
        if errorCatcher(lambda _: product.select_one('div.short_info span:last-of-type').text != 'Ру',
                        lambda _: True, None)]

if __name__ == '__main__':
    instance = Ihromag()
    # c = list(instance.dataSelectors.keys()) + ['url']
    # print(c)
    with open('./page.txt', 'r', encoding='utf-8') as f:
        html = f.read()
    print(instance.extractCatalogData(html))
    print(instance.fieldnames)
    