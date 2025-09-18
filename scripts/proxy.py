import random

class Proxy:
    def __init__(self, proxy_filepath: str):
        self.proxies = []

        # restructure proxies in correct way
        with open(proxy_filepath, 'r') as f:
            text = f.read()

        rows = text.split('\n')[:-1]
        for row in rows:
            address, port, username, password = row.split(':')
            self.proxies.append(f'http://{username}:{password}@{address}:{port}/')

    def get(self, poolSize: int) -> str:
        """Return a random proxy from list of proxies
        with lenght equal poolSize"""

        if poolSize < 1:
            raise ValueError('poolSize value must be a positive integer.')
        elif poolSize > len(self.proxies):
            poolSize = len(self.proxies)

        return random.choice(self.proxies[:poolSize])
    
    def proxyForRequests(self, poolSize: int) -> dict:
        proxy = self.get(poolSize)
        return {'http': proxy, 'https': proxy}
        

if __name__ == "__main__":
    proxy_filepath = r'C:\Users\User\Jupyter Folder\Webshare 10 proxies.txt'
    p = Proxy(proxy_filepath)
    print(p.get(2))