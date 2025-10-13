import random

class Proxy:
    def __init__(self, proxy_filepath: str):
        self.proxies: list
        
        self.setProxies(proxy_filepath)
        self.poolSize = len(self.proxies)
    
    def setProxies(self, proxy_filepath: str):
        self.proxies = []

        # restructure proxies in correct way
        with open(proxy_filepath, 'r') as f:
            text = f.read()

        rows = text.split('\n')
        for row in rows:
            address, port, username, password = row.split(':')
            self.proxies.append(f'http://{username}:{password}@{address}:{port}/')

    def setPoolSize(self, newSize: int):
        self.poolSize = newSize     

    def get(self, poolSize: int = None) -> str:
        """Return a random proxy from list of proxies
        with lenght equal poolSize"""

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
        

if __name__ == "__main__":
    proxy_filepath = r'C:\Users\User\Jupyter Folder\Webshare 10 proxies.txt'
    p = Proxy(proxy_filepath)
    print(p.get(2))