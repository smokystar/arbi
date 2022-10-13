from aiogram import types, Dispatcher
from create import dp, bot
from buttons import key_menu_client, tasks_menu
from aiogram import types
from aiogram.dispatcher import Dispatcher
# from postgres import con, cur
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import psycopg2
from scheme_parser import scheme_analizer
import datetime
import time
import requests
import pandas as pd
import re

con = psycopg2.connect(user="uhwgxkaboaglce",
                                password="75db761e1367ffdf929f791edc4dcd1a58936cbe3fa9e87c920ca16338f2374c",
                                host="ec2-52-48-159-67.eu-west-1.compute.amazonaws.com",
                                port="5432",
                                database="d5ehlrsmpq329l")
cur = con.cursor()

api_key = '1QrbAnjDYWcnmKoQYVn2ZSphucr4yXZtWEwUATG103rqfgJqG0VZ5kW7vdtMIS0Q'
secret_key = 'IU08Ye3WRhrjBEZl28vA9CN3TWL2fLSEv1XMZA8kYjmASbWOPpvVwhXfF6s6WQyS'
client = Client(api_key, secret_key)


from aiogram.dispatcher.filters import Text
admin_id = 394652149

n = 0

async def start_cmd(message: types.Message):
    await bot.send_message(message.from_user.id, 'Hello!', reply_markup=key_menu_client)


async def mainmenu(message: types.Message):
    await bot.send_message(message.from_user.id, 'Main Menu', reply_markup=tasks_menu)


async def scan(message: types.Message):
    user_id = message.from_user.id
    while n < 10:
        try:
            await scheme_analizer(1.04, admin_id)
            time.sleep(30)
        except Exception as e:
            await bot.send_message(message.from_user.id, f'Error occured!\n\n {e}')


def register_handlers_client_partners(dp: Dispatcher):
    dp.register_message_handler(start_cmd, commands=['start'])
    dp.register_message_handler(start_cmd, Text(equals='start', ignore_case=True))
    dp.register_message_handler(mainmenu, commands=['Main menu'])
    dp.register_message_handler(mainmenu, Text(equals='Main menu', ignore_case=True))
    dp.register_message_handler(scan, commands=['activate'])
    dp.register_message_handler(scan, Text(equals='activate', ignore_case=True))


