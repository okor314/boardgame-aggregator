import random
import os
import requests
from dotenv import load_dotenv

load_dotenv('./.env')
PROXY_API_KEY = os.getenv('PROXY_API_KEY')
PROXY_PLAN_ID = os.getenv('PROXY_PLAN_ID')

class Proxy:
    def __init__(self, proxy_api_url: str = ''):
        self.proxies = []
        self.proxyDicts = []
        
        self.setProxies(proxy_api_url)
        self.poolSize = len(self.proxies)
    
    def setProxies(self, proxy_api_url: str):
        self.proxies = []
        self.proxyDicts = []
        if proxy_api_url == '': return

        # restructure proxies in correct way
        response = requests.get(proxy_api_url,
                                headers={"Authorization": PROXY_API_KEY},
                                params={"plan_id": PROXY_PLAN_ID})
        response = response.json()

        self.proxyDicts = [
            {
                'address': proxy.get('proxy_address'),
                'port': proxy.get('port'),
                'username': proxy.get('username'),
                'password': proxy.get('password')
            }
            for proxy in response.get('results') if proxy.get('valid')
        ]

        self.proxies = [
            f'http://{proxy['username']}:{proxy['password']}@{proxy['address']}:{proxy['port']}/'
            for proxy in self.proxyDicts
        ]

    def setPoolSize(self, newSize: int):
        self.poolSize = newSize     

    def get(self, poolSize: int = None) -> str:
        """Return a random proxy from list of proxies
        with lenght equal poolSize"""
        if self.proxies == []: return None

        if poolSize is None:
            poolSize = self.poolSize
        elif poolSize < 1:
            raise ValueError('poolSize value must be a positive integer.')
        elif poolSize > len(self.proxies):
            poolSize = len(self.proxies)

        return random.choice(self.proxies[:poolSize])
    
    def proxyForRequests(self, poolSize: int = None) -> dict:
        proxy = self.get(poolSize)
        return {'http': proxy, 'https': proxy}
        
    def proxyForPlaywright(self, poolSize: int = None) -> dict:
        if self.proxies == []: 
            return {'server': None, 'username': None, 'password': None}

        if poolSize is None:
            poolSize = self.poolSize
        elif poolSize < 1:
            raise ValueError('poolSize value must be a positive integer.')
        elif poolSize > len(self.proxies):
            poolSize = len(self.proxies)

        proxy = random.choice(self.proxyDicts[:poolSize])
        server = f'http://{proxy['address']}:{proxy['port']}'
        username = proxy['username']
        password = proxy['password']
        return {'server': server, 'username': username, 'password': password}

if __name__ == "__main__":
    proxy_filepath = r'C:\Users\User\Jupyter Folder\Webshare 10 proxies.txt'
    p = Proxy(proxy_filepath)
    print(p.get(2))