import random

class Proxy:
    def __init__(self, proxy_filepath: str = ''):
        self.proxies = []
        self.proxyDicts = []
        
        self.setProxies(proxy_filepath)
        self.poolSize = len(self.proxies)
    
    def setProxies(self, proxy_filepath: str):
        self.proxies = []
        self.proxyDicts = []
        if proxy_filepath == '': return

        # restructure proxies in correct way
        with open(proxy_filepath, 'r') as f:
            text = f.read()

        rows = text.split('\n')
        for row in rows:
            address, port, username, password = row.split(':')
            self.proxies.append(f'http://{username}:{password}@{address}:{port}/')
            self.proxyDicts.append({'username': username, 'password': password, 'address': address, 'port': port})

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