# boardgame-aggregator

## Description
**boardgame-aggregator** is a Python-based scraper that collects board game data from multiple e-commerce websites.
The scraper extracts information such as game titles, prices, availability, and links to product pages, and stores it in a **PostgreSQL** database.

The stored data is later used by a companion [Telegram bot](https://github.com/okor314/boardgame-bot)
 to quickly find the **cheapest available version** of any board game across supported stores.

 ---

## Technologies
- **Python 3.10+** — main language of the scraper  
- **Requests / BeautifulSoup** — for fetching and parsing HTML  
- **PostgreSQL** — database for storing scraped data  
- **psycopg2** — interaction with PostgreSQL  
  
---
