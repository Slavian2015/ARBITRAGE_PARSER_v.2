import json
import concurrent.futures
import requests
from random import choice
from fake_useragent import UserAgent
import os
import pandas as pd
from functools import reduce



main_path_data = os.path.abspath(r"C:/inetpub/wwwroot/WBW/data")

a_file = open(main_path_data + "\\keys.json", "r")
json_object = json.load(a_file)
a_file.close()

input_hot_key = json_object["3"]['key']
input_hot_api = json_object["3"]['api']

input_live_key = json_object["2"]['key']
input_live_api = json_object["2"]['api']

input_a_key = json_object["1"]['key']
input_a_api = json_object["1"]['api']

def hot_w():
    import hashlib
    if input_hot_key != "Api key" and input_hot_api != "Api secret":
        str2hash = 'api_key={}&assets=["BTC","ETH","ZEC","USDT","LTC","XRP","PZM","XLM"]&secret_key={}'.format(input_hot_key,
                                                                                                               input_hot_api)
        result = hashlib.md5(str2hash.encode())
        sign = result.hexdigest().upper()
        url = 'https://api.hotbit.io/api/v1/balance.query?api_key={}&assets=["BTC","ETH","ZEC","USDT","LTC","XRP","PZM","XLM"]&sign={}'.format(
            input_hot_key,
            sign)
        return url
    else:
        return ''
def live_w(self):
    import hmac
    if input_live_key != "Api key" and input_live_api != "Api secret":
        sign = hmac.new(input_live_api.encode(), digestmod='sha256').hexdigest().upper()
        headers = {
            'Api-key': input_live_key,
            'Sign': sign,
        }
        return headers
    else:
        return {}
def alfa_w(self):
    import hmac
    from time import time
    if input_a_key != "Api key" and input_a_api != "Api secret":
        msg = input_a_key
        sign = hmac.new(input_a_api.encode(), msg.encode(), digestmod='sha256').hexdigest()

        return {
            'X-KEY': input_a_key,
            'X-SIGN': sign,
            'X-NONCE': str(int(time() * 1000)),
        }
    else:
        return {}

url_Alfa = 'https://btc-alpha.com/api/v1/wallets'
url_Hot = hot_w()
url_Live = 'https://api.livecoin.net/payment/balances'

urls = [
url_Alfa,
url_Hot,
url_Live
]

def kurs():
    out = dict()
    CONNECTIONS = 100
    TIMEOUT = 3

    pro = ['94.154.208.248:80', '89.252.12.88:80', '13.66.220.17:80', '104.45.11.83:80']
    ua = UserAgent()
    proxy = choice(pro)
    PARAMS = {'User-Agent': ua.random, 'http': proxy, 'https': proxy}
    PARAMS_alfa = alfa_w({})
    # PARAMS_hot = hot_w({})
    PARAMS_live = live_w({})

    def load_url(url, timeout, params, PARAM_alfa, PARAM_live):
        if url == 'https://btc-alpha.com/api/v1/wallets':
            ans = requests.get(url, headers=PARAM_alfa, timeout=timeout)
            return url, ans.json()
        elif url == input_hot_api:
            ans = requests.get(url, timeout=timeout)
            return url, ans.json()
        elif url == 'https://api.livecoin.net/payment/balances':
            ans = requests.get(url, headers=PARAM_live, timeout=timeout)
            return url, ans.json()
        else:
            ans = requests.get(url, data=params, timeout=timeout)
            return url, ans.json()

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_url = (executor.submit(load_url, url, TIMEOUT, PARAMS, PARAMS_alfa, PARAMS_live) for url in urls)

        for future in concurrent.futures.as_completed(future_to_url):
            try:
                data = future.result()
                # print(data)
            except Exception as exc:
                data = str(type(exc))
            finally:
                out.update({data[0]:data[1]})

    return out


def NewBalance():

    dictionary2 = json.dumps(kurs())
    dictionary = json.loads(dictionary2)

    def wall_a():
        wallet_a = {}
        wallet_as = []
        for k, v in dictionary.items():
            if 'api/v1/wallets' in f'**{k}**':
                Alfa = dictionary[k]
                wallet_as.append('Ok')
                for i in Alfa:
                    wallet_a.update({i['currency']: (float(i['balance']) - float(i['reserve']))})
            else:
                pass
        if not wallet_as:
            wallet_as.append(' - - Error - -')
        else:
            pass
        return wallet_a, wallet_as
    def wall_h():
        wallet_h = {}
        wallet_hs =[]
        for k, v in dictionary.items():
            if 'balance.query' in f'**{k}**':
                wallet_hs.append('Ok')
                Hot = dictionary[k]
                for i in Hot['result']:
                    wallet_h.update({i: Hot['result'][i]['available']})
            else:
                pass
        if not wallet_hs:
            wallet_hs.append(' - - Error - -')
        else:
            pass
        return wallet_h, wallet_hs
    def wall_l():
        wallet_l = {}
        wallet_ls = []
        for k, v in dictionary.items():
            if 'payment/balances' in f'**{k}**':
                wallet_ls.append('Ok')
                Live = dictionary[k]
                for i in Live:
                    if i['type'] == "available" and i['value'] > 0:
                        wallet_l.update({i['currency']: i['value']})
            else:
                pass
        if not wallet_ls:
            wallet_ls.append(' - - Error - -')
        else:
            pass
        return wallet_l, wallet_ls

    Alfa2 = wall_a()
    Hot2 = wall_h()
    Live2 = wall_l()

    def balance():

        dfa = pd.DataFrame(Alfa2[0].items(), columns=['Valuta', 'alfa'])
        dfh = pd.DataFrame(Hot2[0].items(), columns=['Valuta', 'hot'])
        dfl = pd.DataFrame(Live2[0].items(), columns=['Valuta', 'live'])

        data_frames = [dfl, dfh, dfa]
        valuta = reduce(lambda left, right: pd.merge(left, right, on=['Valuta'],
                                                     how='outer'), data_frames).fillna('0')

        valuta['alfa'] = valuta['alfa'].apply(pd.to_numeric, errors='coerce')
        valuta['live'] = valuta['live'].apply(pd.to_numeric, errors='coerce')
        valuta['hot'] = valuta['hot'].apply(pd.to_numeric, errors='coerce')

        fil1 = valuta[valuta['Valuta'] == 'USD']
        fil2 = valuta[valuta['Valuta'] == 'USDT']

        al = (float(fil1.iloc[0]['alfa']) + float(fil2.iloc[0]['alfa']))
        li = (float(fil1.iloc[0]['live']) + float(fil2.iloc[0]['live']))
        ho = (float(fil1.iloc[0]['hot']) + float(fil2.iloc[0]['hot']))

        valuta = valuta.append({'Valuta': 'USDT + USD', 'alfa': al, 'live': li, 'hot': ho}, ignore_index=True)

        valuta.loc[:, "Summa"] = (valuta.loc[:, "alfa"] + valuta.loc[:, "live"] + valuta.loc[:, "hot"])
        valuta = valuta[['Valuta', 'alfa', 'live', 'hot', 'Summa']]
        valuta = valuta[(valuta['Summa'] != 0)]

        return valuta

    valuta_main = balance()
    valuta_main.to_csv(main_path_data + "\\balance.csv", index=False)

    return
