from scripts.scraper import Scraper, Context
from scripts.proxy import Proxy
from scripts.site_classes import (
    Gameland,
    Geekach,
    Woodcat,
    Ihromag,
    LordOfBoards
)
from scripts.notify_users import sendNotifications

from database.game_database import updateGameTable
from database.site_database import upsertTable
from database.history_database import updateHistoryTable
from database.subscriptions import getURLsToScrape
from database.utils import getSitesToScrape, updateScrapingDate
 
from datetime import date
import logging


PROXY_URL = r'https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25'

SITES = [Gameland, Geekach, Woodcat, Ihromag, LordOfBoards]
SITES_DICT = {site.siteName: site for site in SITES}

SUBSCRIPTIONS_SCRAPE_DAY = 1    # Tuesday

logger = logging.getLogger(__name__)

def run_site(site, proxy, context, today):
    scraper = Scraper(site, proxy, context)
    table_name = site.siteName

    try:
        if today == SUBSCRIPTIONS_SCRAPE_DAY:
            links = getURLsToScrape(table_name)
            scraper.scrapeLinks(links)
        else:
            scraper.scrape()

        upsertTable(table_name, f"./data/{table_name}.csv")
        updateScrapingDate(site.siteName)

    except Exception:
        logger.exception("Failed processing site %s", table_name)


def main():
    proxy = Proxy(PROXY_URL)
    context = Context(workers=7, sleep_break=4)
    today = date.today().weekday()

    sites_to_scrape = (SITES if today == SUBSCRIPTIONS_SCRAPE_DAY 
                       else [SITES_DICT[site_name] for site_name in getSitesToScrape()])

    for site in sites_to_scrape:
        run_site(site, proxy, context, today)

    updateGameTable()
    updateHistoryTable()
    sendNotifications()


if __name__ == '__main__':
    main()