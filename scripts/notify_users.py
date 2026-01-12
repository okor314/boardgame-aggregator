from dotenv import load_dotenv
import os
import concurrent.futures
import requests
from typing import Iterable

from database.subscriptions import getUsersSubscribedOn, updateSubsPrice

load_dotenv('test.env')

BOT_TOKEN = os.getenv('BOT_TOKEN')
BASE_URL = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'

def textFromGameDetails(game_details: dict) -> str:
    savings = game_details['old_price'] - game_details['new_price']
    discount = round(savings / game_details['old_price']* 100) 

    return (f'<a href="{game_details['url']}">{game_details['title']}</a>\n'\
            f'Ціна: <u>{game_details['new_price']}  грн.</u> (<s>{game_details['old_price']} грн.</s>)\n'\
            f'Заощадження: {savings} грн. ({discount}%)')

def textForUser(game_ids: Iterable[int], game_text_mapping: dict[int, str]):
    message_parts = ['Зменшилася ціна на ігри, за якими ви стежите:']
    for game_id in game_ids:
        if game_text_mapping.get(game_id):
            message_parts.append(game_text_mapping.get(game_id))
    
    return '\n\n'.join(message_parts)

def MapGamesToText(games_details: dict[int, dict]) -> dict[int, str]:
    return {game_id: textFromGameDetails(details) for game_id, details in games_details.items()}

def validatePriceDifference(games_details: dict[int, dict]) -> dict[int, dict]:
    return {game_id: details for game_id, details in games_details.items()
            if details['old_price'] - details['new_price'] >= 50}

def prepareMessages():
    """Function update price in subscriptions table and
    return list of users who need to be notified and list
    of respective messages."""

    games = updateSubsPrice()
    games = validatePriceDifference(games)
    game_text_mapping = MapGamesToText(games)

    users = getUsersSubscribedOn(list(games.keys()))
    users_messages = {user: textForUser(game_ids, game_text_mapping) 
                      for user, game_ids in users.items()}
    
    return list(users_messages.keys()), list(users_messages.values())

def push(text: str, chat_id: int):
    playload = {
        'text': text,
        'chat_id': chat_id,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True
    }

    response = requests.post(BASE_URL, data=playload)
    return response.text

def sendNotifications():
    users, messages = prepareMessages()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(push, messages, users)


if __name__ == '__main__':
    sendNotifications()