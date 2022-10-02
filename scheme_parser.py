# from binance_api_keys import api_key, secret_key
from binance import Client
# from scan_bot.postgres import cur, con
import requests
from scan_bot.arbi_bot.create import dp, bot
# from scan_bot.create import dp, bot
import pandas as pd

import json
from datetime import datetime
from scan_bot.everything.postgres import cur, con
# user_id = 394652149
api_key = '1QrbAnjDYWcnmKoQYVn2ZSphucr4yXZtWEwUATG103rqfgJqG0VZ5kW7vdtMIS0Q'
secret_key = 'IU08Ye3WRhrjBEZl28vA9CN3TWL2fLSEv1XMZA8kYjmASbWOPpvVwhXfF6s6WQyS'
client = Client(api_key, secret_key)
chck = 0
admin_id = 394652149

async def scheme_analizer(prmin, user_id):
    end_counter = 0
    id_slvr = 0
    best_schemes_list = []

    usdt_buy_list = []
    data_usdt = {
        "asset": "USDT",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "BUY"
    }

    headers_usdt = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_usdt = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_usdt,
                           json=data_usdt)
    df_usdt = r_usdt.json()['data']
    # print(df_usdt)

    for i in df_usdt:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        if len(trademethod) > 1:
            # print('double check', trademethod[0]['tradeMethodName'])
            # print('len trademethod', len(trademethod))
            for i in trademethod:
                # print('i check trademethod: ', i)
                aa = i['tradeMethodName']
                # print('aa', aa)
                if aa == 'Advcash':
                    tradeMethodName = 'Advcash'
                    continue
                else:
                    tradeMethodName = trademethod[0]['tradeMethodName']

        else:
            tradeMethodName = trademethod[0]['tradeMethodName']
        sum_tr = tradeMethodName
        # print('sum_tr: ', sum_tr)
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        usdt_buy_list.append(dict)
    # print('usdt_buy_list', usdt_buy_list)

    rub_sell_list = []
    data_rub_sell = {
        "asset": "RUB",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "SELL"
    }

    headers_rub_sell = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_rub_sell = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_rub_sell,
                               json=data_rub_sell)
    df_rub_sell = r_rub_sell.json()['data']
    # print(df_usdt)

    for i in df_rub_sell:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        rub_sell_list.append(dict)
    # print('rub_sell_list', rub_sell_list)

    usdt_sell_list = []
    data_sell_usdt = {
        "asset": "USDT",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "SELL"
    }

    headers_sell_usdt = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_usdt_sell = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search',
                                headers=headers_sell_usdt, json=data_sell_usdt)
    df_usdt_sell = r_usdt_sell.json()['data']
    # print(df_usdt)

    for i in df_usdt_sell:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        usdt_sell_list.append(dict)
    # print('usdt_sell_list', usdt_sell_list)

    rub_buy_list = []
    data_rub_buy = {
        "asset": "RUB",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "BUY"
    }

    headers_rub_buy = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_rub_buy = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_rub_buy,
                              json=data_rub_buy)
    df_rub_buy = r_rub_buy.json()['data']
    # print(df_usdt)

    for i in df_rub_buy:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        rub_buy_list.append(dict)
    # print('rub_buy_list', rub_buy_list)

    busd_sell_list = []
    data_busd_sell = {
        "asset": "BUSD",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "SELL"
    }

    headers_busd_sell = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_busd_sell = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search',
                                headers=headers_busd_sell, json=data_busd_sell)
    df_busd_sell = r_busd_sell.json()['data']
    # print(df_usdt)

    for i in df_busd_sell:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        busd_sell_list.append(dict)
    # print('busd_sell_list', busd_sell_list)

    busd_buy_list = []
    data_busd_buy = {
        "asset": "BUSD",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "BUY"
    }

    headers_busd_buy = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_busd_buy = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_busd_buy,
                               json=data_busd_buy)
    df_busd_buy = r_busd_buy.json()['data']
    # print(df_usdt)

    for i in df_busd_buy:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        busd_buy_list.append(dict)
    # print('busd_buy_list', busd_buy_list)

    btc_sell_list = []
    data_btc_sell = {
        "asset": "BTC",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "SELL"
    }

    headers_btc_sell = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_btc_sell = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_btc_sell,
                               json=data_btc_sell)
    df_btc_sell = r_btc_sell.json()['data']
    # print(df_usdt)

    for i in df_btc_sell:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        btc_sell_list.append(dict)
    # print('btc_sell_list', btc_sell_list)

    btc_buy_list = []
    data_btc_buy = {
        "asset": "BTC",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "BUY"
    }

    headers_btc_buy = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_btc_buy = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_btc_buy,
                              json=data_btc_buy)
    df_btc_buy = r_btc_buy.json()['data']
    # print(df_usdt)

    for i in df_btc_buy:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        btc_buy_list.append(dict)
    # print('btc_buy_list', btc_buy_list)

    eth_sell_list = []
    data_eth_sell = {
        "asset": "ETH",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "SELL"
    }

    headers_eth_sell = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_eth_sell = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_eth_sell,
                               json=data_eth_sell)
    df_eth_sell = r_eth_sell.json()['data']
    # print(df_usdt)

    for i in df_eth_sell:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        eth_sell_list.append(dict)
    # print('eth_sell_list', eth_sell_list)

    eth_buy_list = []
    data_eth_buy = {
        "asset": "ETH",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "BUY"
    }

    headers_eth_buy = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_eth_buy = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_eth_buy,
                              json=data_eth_buy)
    df_eth_buy = r_eth_buy.json()['data']
    # print(df_usdt)

    for i in df_eth_buy:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        eth_buy_list.append(dict)
    # print('eth_buy_list', eth_buy_list)

    bnb_sell_list = []
    data_bnb_sell = {
        "asset": "BNB",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "SELL"
    }

    headers_bnb_sell = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_bnb_sell = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_bnb_sell,
                               json=data_bnb_sell)
    df_bnb_sell = r_bnb_sell.json()['data']
    # print(df_usdt)

    for i in df_bnb_sell:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        bnb_sell_list.append(dict)
    # print('bnb_sell_list', bnb_sell_list)

    bnb_buy_list = []
    data_bnb_buy = {
        "asset": "BNB",
        "fiat": "RUB",
        "merchantCheck": False,
        "page": 1,
        "payTypes": [],
        "publisherType": None,
        "rows": 20,
        "tradeType": "BUY"
    }

    headers_bnb_buy = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "1233",
        "content-type": "application/json",
        "Host": "p2p.binance.com",
        "Origin": "https://p2p.binance.com",
        "Pragma": "no-cache",
        "TE": "Trailers",
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    r_bnb_buy = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search', headers=headers_bnb_buy,
                              json=data_bnb_buy)
    df_bnb_buy = r_bnb_buy.json()['data']
    # print(df_usdt)

    for i in df_bnb_buy:
        user_unique_link = i['advertiser']['userNo']
        # print(user_unique_link)
        merchant_price = float(i['adv']['price'])
        # print(merchant_price)
        trademethod = i['adv']['tradeMethods']
        tradeMethodName = trademethod[0]['tradeMethodName']
        # print(tradeMethodName)
        dict = {'price': merchant_price, 'paymethod': tradeMethodName, 'link': user_unique_link}
        bnb_buy_list.append(dict)
    # print('bnb_buy_list', bnb_buy_list)

    price_usdt = client.get_ticker(symbol='USDTRUB')
    usdt_last_price = float(price_usdt['lastPrice'])
    # print(usdt_last_price)
    price_busd = client.get_ticker(symbol='BUSDRUB')
    busd_last_price = float(price_busd['lastPrice'])
    # print(busd_last_price)
    price_btc = client.get_ticker(symbol='BTCRUB')
    btc_last_price = float(price_btc['lastPrice'])
    # print(btc_last_price)
    price_eth = client.get_ticker(symbol='ETHRUB')
    eth_last_price = float(price_eth['lastPrice'])
    # print(eth_last_price)
    price_bnb = client.get_ticker(symbol='BNBRUB')
    bnb_last_price = float(price_bnb['lastPrice'])
    # print(bnb_last_price)
    price_ub = client.get_ticker(symbol='BUSDUSDT')
    ub_last_price = float(price_ub['lastPrice'])
    # print(ub_last_price)
    sum = 1000
    for i in usdt_buy_list:
        for j in rub_sell_list:
            a = i['price']
            b = j['price']
            a_pay = i['paymethod']
            b_pay = j['paymethod']
            slvr = sum / a * usdt_last_price * b

            if slvr > prmin * sum:
                id_slvr += 1
                best_1 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                          'link_b': j['link'], 'slvr': slvr}
                best_schemes_list.append(best_1)

                # await bot.send_message(user_id, f'SUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
            else:
                # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                pass

        #     max_k_list.append(k)
        # print(max(max_k_list))

        for i in busd_buy_list:
            for j in rub_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * busd_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_2 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                              'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_2)
                    # await bot.send_message(user_id, f'SUCCESS: i[price]: {a}, busd_last_price: {busd_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, busd_last_price: {busd_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in btc_buy_list:
            for j in rub_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * btc_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_3 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                              'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_3)
                    # await bot.send_message(user_id, f'SUCCESS: i[price]: {a}, btc_last_price: {btc_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, btc_last_price: {btc_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in eth_buy_list:
            for j in rub_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * eth_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_4 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                              'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_4)
                    # await bot.send_message(user_id, f'SUCCESS: i[price]: {a}, eth_last_price: {eth_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, eth_last_price: {eth_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in bnb_buy_list:
            for j in rub_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * bnb_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_5 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                              'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_5)
                    # await bot.send_message(user_id, f'SUCCESS: i[price]: {a}, bnb_last_price: {bnb_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, bnb_last_price: {bnb_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in rub_buy_list:
            for j in usdt_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a / usdt_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_6 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                              'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_6)
                    # print(f'SUCCESS IN REVERSE: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in rub_buy_list:
            for j in busd_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a / busd_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_7 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                              'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_7)
                    # print(f'SUCCESS IN REVERSE: i[price]: {a}, busd_last_price: {busd_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, busd_last_price: {busd_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in rub_buy_list:
            for j in btc_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a / btc_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_8 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                              'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_8)
                    # print(f'SUCCESS IN REVERSE: i[price]: {a}, btc_last_price: {btc_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, btc_last_price: {btc_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in rub_buy_list:
            for j in eth_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a / eth_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_9 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'], 'b_pay': j['paymethod'],
                              'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_9)
                    # print(f'SUCCESS IN REVERSE: i[price]: {a}, eth_last_price: {eth_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, eth_last_price: {eth_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in rub_buy_list:
            for j in bnb_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a / bnb_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_10 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_10)
                    # print(f'SUCCESS IN REVERSE: i[price]: {a}, bnb_last_price: {bnb_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, bnb_last_price: {bnb_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in usdt_buy_list:
            for j in usdt_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_11 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_11)
                    # await bot.send_message(user_id, f'BS SUCCESS USDT: i[price]: {a}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in busd_buy_list:
            for j in busd_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_12 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_12)
                    # await bot.send_message(user_id, f'BS SUCCESS BUSD: i[price]: {a}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in btc_buy_list:
            for j in btc_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_13 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_13)
                    # await bot.send_message(user_id, f'BS SUCCESS BTC: i[price]: {a}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in eth_buy_list:
            for j in eth_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_14 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_14)
                    # await bot.send_message(user_id, f'BS SUCCESS ETH: i[price]: {a}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in bnb_buy_list:
            for j in bnb_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_15 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_15)
                    # await bot.send_message(user_id, f'BS SUCCESS BNB: i[price]: {a}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in rub_buy_list:
            for j in rub_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_16 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_16)
                    # await bot.send_message(user_id, f'BS SUCCESS RUB: i[price]: {a}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in usdt_buy_list:
            for j in busd_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a * ub_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_17 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_17)
                    # await bot.send_message(user_id, f'BS SUCCESS USDT->BUSD: i[price]: {a}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

        for i in busd_buy_list:
            for j in usdt_sell_list:
                a = i['price']
                b = j['price']
                a_pay = i['paymethod']
                b_pay = j['paymethod']
                slvr = sum / a / ub_last_price * b
                if slvr > prmin * sum:
                    id_slvr += 1
                    best_18 = {'id_slvr': id_slvr, 'a_pay': i['paymethod'], 'link_a': i['link'],
                               'b_pay': j['paymethod'],
                               'link_b': j['link'], 'slvr': slvr}
                    best_schemes_list.append(best_18)
                    # await bot.send_message(user_id, f'BS SUCCESS BUSD->USDT: i[price]: {a}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                else:
                    # await bot.send_message(user_id, f'%%UNSUCCESS: i[price]: {a}, usdt_last_price: {usdt_last_price}, j[price]: {b}, slvr: {slvr}, a_payment_method: {a_pay}, b_payment_method: {b_pay}')
                    pass

    if len(best_schemes_list) != 0:
        max_k_list = []
        k_max = 0
        df = pd.DataFrame(best_schemes_list)
        # for key in best_schemes_list:
        #     k = key['slvr']
        #     k_id = key['id_slvr']
        #     print('k', k)
        #     if k > k_max:
        #         k_max = k
        #         k_max_id = k_id
        # print(f'new max k: {k_max}, min_k_id: {k_max_id}')
        # print(best_schemes_list[0]['id_slvr']==k_max_id)
        # str = df[df['slvr'] == df['slvr'].max()]
        str = df.sort_values(by='slvr', ascending=False)
        strstr = str.head(1)
        # print(strstr)
        # p1 = strstr.iloc[0]['id_slvr']
        # print(p1)
        # print('\n', 'ANY B_PAY', '\n')
        # print(strstr.iloc[0].to_json())
        try:
            max_message = strstr.iloc[0].to_json()
            items_max = json.loads(max_message)
            link_max_a = items_max['link_a']
            link_max_b = items_max['link_b']
            pay_a_max = items_max['a_pay']
            pay_b_max = items_max['b_pay']
            slvr_proc_max = (float(items_max['slvr']) - 1000) / (1000 / 100)
            round_slvr_proc_max = round(slvr_proc_max, 3)
            cur.execute(f'SELECT COUNT(*) FROM best_profit_procents')
            best_proc_id = cur.fetchone()[0] + 1
            datetime_prm = datetime.now()
            cur.execute(f"INSERT INTO best_profit_procents(scheme_id, time_now, procent, a_pay, b_pay)"
                        f" VALUES ({best_proc_id}, '{datetime_prm}', {round_slvr_proc_max}, '{pay_a_max}', '{pay_b_max}')")
            con.commit()
            await bot.send_message(admin_id, f"Profit scheme just has been found! Profit before taxes: {round_slvr_proc_max}%\n\n"
                  f"You need to buy with {pay_a_max} by this link:\n"
                  f"https://p2p.binance.com/ru/advertiserDetail?advertiserNo={link_max_a}\n\n"
                  f"And need to sell with {pay_b_max} by this link:\n"
                  f"https://p2p.binance.com/ru/advertiserDetail?advertiserNo={link_max_b}")

        except Exception as e:
            await bot.send_message(admin_id, f'error: {e}')

        # print('\n', 'QIWI B_PAY', '\n')
        try:

        # filter_qiwi=(df[df['slvr'] == df['slvr'].max()])&(df['b_pay']=='QIWI')
            filter_qiwi = (str['b_pay'] == 'QIWI')
            # str_qiwi=str[filter_qiwi].sort_values(by='slvr',ascending=False)
            str_qiwi = str[filter_qiwi]

            qiwi_message = str_qiwi.iloc[0].to_json()
            items_qiwi = json.loads(qiwi_message)
            link_a = items_qiwi['link_a']
            link_b = items_qiwi['link_b']
            pay_a = items_qiwi['a_pay']
            pay_b = items_qiwi['b_pay']
            slvr_proc = (float(items_qiwi['slvr']) - 1000) / (1000/100)
            round_slvr_proc = round(slvr_proc, 3)
            cur.execute(f'SELECT COUNT(*) FROM best_profit_procents')
            best_proc_id = cur.fetchone()[0] + 1
            datetime_prm = datetime.now()
            cur.execute(f"INSERT INTO best_profit_procents(scheme_id, time_now, procent, a_pay, b_pay)"
                        f" VALUES ({best_proc_id}, '{datetime_prm}', {round_slvr_proc}, '{pay_a}', '{pay_b}')")
            con.commit()
            await bot.send_message(admin_id, f"Profit scheme just has been found! Profit before taxes: {round_slvr_proc}%\n\n"
                  f"You need to buy with {pay_a} by this link:\n"
                  f"https://p2p.binance.com/ru/advertiserDetail?advertiserNo={link_a}\n\n"
                  f"And need to sell with {pay_b} by this link:\n"
                  f"https://p2p.binance.com/ru/advertiserDetail?advertiserNo={link_b}")
        except Exception as e:
            await bot.send_message(admin_id, f'error: {e}')
    else:
        await bot.send_message(admin_id, 'Nothing...(')
# try:
#     scheme_analizer(1.05, 394652149)
#     await bot.send_message(admin_id, 'success!')
# except Exception as e:
#     await bot.send_message(admin_id, f'error: {e}')
