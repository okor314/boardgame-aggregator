import unittest
from scripts.scraper import Scraper
from scripts.site_classes import HoroshoSite

class TestScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = Scraper(HoroshoSite)
        self.scraper.failedURLs = [
            'https://gameland.com.ua/nanty-narking.-veliki-spodivannya-ukr./',
            'https://gameland.com.ua/bytva-restoraniv-rival-restaurants/',
            'https://gameland.com.ua/stambul.-povne-vydannia-istanbul-big-box/'
        ]
        self.catalogData = [
            {'id': '1497490289', 'in_stock': True, 'price': 1470, 'url': 'https://gameland.com.ua/bytva-restoraniv-rival-restaurants/'}, 
            {'id': '1497490961', 'in_stock': False, 'price': 1715, 'url': 'https://gameland.com.ua/stambul.-povne-vydannia-istanbul-big-box/'}, 
            {'id': '1458100540', 'price': 900, 'url': 'https://gameland.com.ua/mikromakro-vbivche-misto-ukr./'}, 
            {'id': '1497491171', 'in_stock': True, 'url': 'https://gameland.com.ua/mikromakro-vbyvche-misto-va-bank-micromacro-crime-city-all-in/'}, 
            {'id': '1497489964', 'in_stock': True, 'price': 370, 'url': 'https://gameland.com.ua/vyshche-suspilstvo-high-society/'}
            ]
        self.scraper.existingURLs = [item['url'] for item in self.catalogData[:-1]]

    def test_getLinks(self):
        links = self.scraper._getLinks(self.catalogData)
        msg_not_in = 'This url should not be in the list'
        msg_in = 'This url should be in the list'
        self.assertNotIn('https://gameland.com.ua/bytva-restoraniv-rival-restaurants/', links, msg_not_in)
        self.assertNotIn('https://gameland.com.ua/stambul.-povne-vydannia-istanbul-big-box/', links, msg_not_in)
        self.assertIn('https://gameland.com.ua/mikromakro-vbivche-misto-ukr./', links, msg_in)
        self.assertIn('https://gameland.com.ua/mikromakro-vbyvche-misto-va-bank-micromacro-crime-city-all-in/', links, msg_in)
        self.assertIn('https://gameland.com.ua/vyshche-suspilstvo-high-society/', links, msg_in)

    def test_merge(self):
        newData = [
            {'title': 'Test 1', 'url': 'https://gameland.com.ua/mikromakro-vbivche-misto-ukr./'},
            {'title': 'Test 2', 'url': 'https://gameland.com.ua/vyshche-suspilstvo-high-society/'},
            {'url': 'https://gameland.com.ua/mikromakro-vbyvche-misto-va-bank-micromacro-crime-city-all-in/', 'failed': True}
        ]
        result = self.scraper._mergeData(newData, self.catalogData)
        result_urls = [res['url'] for res in result]

        self.assertEqual(len(result), 4, 'Wrong number of products')

        self.assertIn('https://gameland.com.ua/bytva-restoraniv-rival-restaurants/', result_urls)
        self.assertIn('https://gameland.com.ua/mikromakro-vbivche-misto-ukr./', result_urls)
        self.assertIn('https://gameland.com.ua/vyshche-suspilstvo-high-society/', result_urls)
        self.assertNotIn('https://gameland.com.ua/mikromakro-vbyvche-misto-va-bank-micromacro-crime-city-all-in/', result_urls)

        self.assertIn(
            {'title': 'Test 1', 'url': 'https://gameland.com.ua/mikromakro-vbivche-misto-ukr./'},
            result)
        self.assertIn(
            {'title': 'Test 2', 'url': 'https://gameland.com.ua/vyshche-suspilstvo-high-society/'},
            result)
        


if __name__ == '__main__':
    unittest.main()