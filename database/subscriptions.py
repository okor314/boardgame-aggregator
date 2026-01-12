from psycopg2.extras import RealDictCursor
from database.utils import get_db
from collections import defaultdict

def getURLsToScrape(tableName):
    try:
        connection = get_db()
        cursor = connection.cursor()
        cursor.execute(f"""SELECT DISTINCT ON (g.id) t.url
                            FROM game AS g
                            JOIN subscription AS s
                            ON g.id = s.game_id
                            JOIN {tableName} t
                            ON g.{tableName}_id = t.id;""")
        return list(row[0] for row in cursor.fetchall())
    except Exception as e:
        return []
    finally:
        connection.close()

def getUsersSubscribedOn(game_id_list: list):
    try:
        connection = get_db()
        cursor = connection.cursor(cursor_factory = RealDictCursor)
        game_ids = ','.join(['%s' for _ in game_id_list])
        cursor.execute(f"""SELECT u.telegram_user_id, sub.game_id
                        FROM subscription sub
                        JOIN bot_user u ON sub.user_id = u.id
                        WHERE game_id IN ({game_ids});""", game_id_list)
        
        subs = cursor.fetchall()
        # Sorting subscriptions by telegram_user_id
        subs_by_user = defaultdict(set)
        for sub in subs:
            subs_by_user[sub['telegram_user_id']].add(sub['game_id'])

        return subs_by_user

    except Exception as e:
        print(e)
        return {}
    finally:
        connection.close()



def updateSubsPrice() -> dict[int, dict]:
    """Update minimal price of a game in table Subscription.
    Return details about games which have been updated."""
    
    try:
        connection = get_db()
        min_price_rows = getMinPrices(connection)

        cursor = connection.cursor(cursor_factory = RealDictCursor)
        # Create temp table with min prices
        cursor.execute("""CREATE TABLE tmp_prices (
                        game_id INT,
                        site_name VARCHAR(20),
                        min_price INT)""")
        cursor.executemany("""INSERT INTO tmp_prices
                           (game_id, site_name, min_price)
                           VALUES (%s, %s, %s)""", min_price_rows)
        
        # Gather info from games which price changed
        updated_games = getGamesDetails(connection)

        # Sorting games by game_id
        games_by_id = {game['game_id']: game for game in updated_games}

        # Updating min price in subscription table
        cursor.execute("""UPDATE subscription sub
                       SET lastminprice = t.min_price
                       FROM tmp_prices t
                       WHERE sub.game_id = t.game_id
                       AND sub.lastminprice != t.min_price""")

        # Deleting temporary table
        connection.commit()
        delTemporaryTables(connection)
        return games_by_id

    except Exception as e:
        print(e)
        return []
    finally:
        connection.close()

def getMinPrices(connection) -> list[tuple]:
    """
    Return list of rows with minimal price of games
    to which users subscribe to. Each row contain 
    game_id, site_name with minimal price, minimal price
    respectivly.
    """
    cursor = connection.cursor()
    statement = """WITH latest_prices AS (
                    SELECT DISTINCT ON (game_id, site_id)
                        game_id,
                        site_id,
                        price
                    FROM history
                    ORDER BY game_id, site_id, checkdate DESC
                ),
                min_prices AS (
                    SELECT DISTINCT ON (game_id)
                        game_id,
                        site_id,
                        price AS min_price
                    FROM latest_prices
                    ORDER BY game_id, price ASC
                )
                SELECT DISTINCT ON (sub.game_id)
                mp.game_id, site.name, mp.min_price
                FROM min_prices mp
                JOIN subscription sub ON sub.game_id = mp.game_id
                JOIN site ON mp.site_id = site.id;
                """
    cursor.execute(statement)
    
    return cursor.fetchall()

def getGamesDetails(connection):
    cursor = connection.cursor(cursor_factory = RealDictCursor)
    cursor.execute("""SELECT name FROM site;""")
    sites = cursor.fetchall()
    
    # Gather info from games which price changed
    cursor.execute("""SELECT * INTO tmp_details FROM (
                    SELECT DISTINCT ON (sub.game_id) 
                        sub.game_id,
                        game.title,
                        sub.lastminprice old_price,
                        tp.min_price new_price,
                        tp.site_name
                    FROM subscription sub
                    JOIN tmp_prices tp ON sub.game_id = tp.game_id AND sub.lastminprice != tp.min_price
                    JOIN game ON game.id = sub.game_id
                    );""")

    # Get urls of games that changed in price
    statments = []
    for site in sites:
        stmt = f"""SELECT det.game_id, t.url
                FROM tmp_details det
                JOIN {site['name']} t ON det.game_id = t.game_id
                WHERE det.site_name = '{site['name']}'"""
        statments.append(stmt)
    
    query = 'SELECT * INTO tmp_urls FROM ('+' UNION '.join(statments)+')'
    cursor.execute(query)

    # Returning games details with urls
    cursor.execute("""SELECT tmpd.*, tmpu.url
                   FROM tmp_details tmpd
                   JOIN tmp_urls tmpu ON tmpd.game_id = tmpu.game_id""")
    games = cursor.fetchall()

    cursor.close()

    return games



def delTemporaryTables(connection):
    cursor = connection.cursor()
    cursor.execute("""SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    AND table_type='BASE TABLE';""")

    tmp_tables = [row[0] for row in cursor.fetchall() if row[0].startswith('tmp_')]

    for table in tmp_tables:
        cursor.execute(f"DROP TABLE {table}")
    cursor.close()
    connection.commit()


if __name__ == '__main__':
    print(getUsersSubscribedOn([1,2,3]))