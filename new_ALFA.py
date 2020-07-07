import json
import concurrent.futures
import requests
import time
from random import choice
from fake_useragent import UserAgent
import os
import pandas as pd

main_path_data = os.path.abspath("./data")


url2 = 'https://btc-alpha.com/api/v1/orderbook/PZM_USD'
url3 = 'https://btc-alpha.com/api/v1/orderbook/PZM_BTC'


urls = [
    url2,
    url3,
]

def refresh():

    def kurs():
        out = dict()
        CONNECTIONS = 100
        TIMEOUT = 2

        pro = ['94.154.208.248:80', '89.252.12.88:80', '13.66.220.17:80', '104.45.11.83:80']
        ua = UserAgent()
        proxy = choice(pro)
        PARAMS = {'User-Agent': ua.random, 'http': proxy, 'https': proxy}

        def load_url(url, timeout, params):
            ans = requests.get(url, data=params, timeout=timeout)
            return url, ans.json()

        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            future_to_url = (executor.submit(load_url, url, TIMEOUT, PARAMS) for url in urls)

            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    data = future.result()
                except Exception as exc:
                    data = str(type(exc))
                finally:
                    out.update({data[0]:data[1]})
        return out

    dictionary2 = json.dumps(kurs())
    dictionary = json.loads(dictionary2)


    def ord(link, val1, val2, n):
        Alfa_sell = {}
        for i in dictionary[link]['sell'][:n]:
            if not Alfa_sell:
                Alfa_sell.update({i['price']: float(i['amount'])})
            else:
                sump = float(i['amount']) + float([*Alfa_sell.values()][-1])
                Alfa_sell.update({i['price']: sump})

        Alfa_buy = {}
        for i in dictionary[link]['buy'][:n]:
            if not Alfa_buy:
                Alfa_buy.update({i['price']: float(i['amount'])})
            else:
                sump = float(i['amount']) + float([*Alfa_buy.values()][-1])
                Alfa_buy.update({i['price']: sump})

        Alfa_sell = [*Alfa_sell.items()][:n]
        Alfa_buy = [*Alfa_buy.items()][:n]

        alfa_PU = []
        for i in Alfa_sell:
            alfa_PU.append(('alfa', val1, val2, 'buy', i[0], i[1]))
        for i in Alfa_buy:
            alfa_PU.append(('alfa', val2, val1, 'sell', i[0], i[1]))




        return alfa_PU


    list = [('https://btc-alpha.com/api/v1/orderbook/PZM_USD', 'USD', 'PZM', 5),('https://btc-alpha.com/api/v1/orderbook/PZM_BTC', 'BTC', 'PZM', 5),('https://btc-alpha.com/api/v1/orderbook/PZM_USDT', 'USDT', 'PZM', 5)]

    for i in list:
        if i[1] == 'USD':
            columns = ['birga', 'valin', 'valout', 'direction', 'rates', 'volume']
            df = pd.DataFrame(ord(i[0],i[1],i[2],i[3]), columns=columns)

            try:
                os.remove(main_path_data + "\\alfa_bd_PU.csv")
                df.to_csv(main_path_data + "\\alfa_bd_PU.csv", index=False, mode="w")
            except Exception as e:
                print('#####   OOOPsss .... DB   ######')
                os.remove(main_path_data + "\\alfa_bd_PU.csv")
                df.to_csv(main_path_data + "\\alfa_bd_PU.csv", index=False, mode="w")
        elif i[1] == 'BTC':
            columns = ['birga', 'valin', 'valout', 'direction', 'rates', 'volume']
            df = pd.DataFrame(ord(i[0],i[1],i[2],i[3]), columns=columns)

            try:
                os.remove(main_path_data + "\\alfa_bd_PB.csv")
                df.to_csv(main_path_data + "\\alfa_bd_PB.csv", index=False, mode="w")
            except Exception as e:
                print('#####   OOOPsss .... DB   ######')
                os.remove(main_path_data + "\\alfa_bd_PB.csv")
                df.to_csv(main_path_data + "\\alfa_bd_PB.csv", index=False, mode="w")
        elif i[1] == 'USDT':
            columns = ['birga', 'valin', 'valout', 'direction', 'rates', 'volume']
            df = pd.DataFrame(ord(i[0],i[1],i[2],i[3]), columns=columns)

            try:
                os.remove(main_path_data + "\\alfa_bd_PUT.csv")
                df.to_csv(main_path_data + "\\alfa_bd_PUT.csv", index=False, mode="w")
            except Exception as e:
                print('#####   OOOPsss .... DB   ######')
                os.remove(main_path_data + "\\alfa_bd_PUT.csv")
                df.to_csv(main_path_data + "\\alfa_bd_PUT.csv", index=False, mode="w")


if __name__ == "__main__":
    while True:
        try:
            t1 = time.time()
            refresh()
            t2 = time.time()
            print("ALL TIME :", t2-t1)
            time.sleep(1)
        except Exception as e:
            print(e)
            time.sleep(5)