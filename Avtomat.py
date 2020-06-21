import pandas as pd
import json
import os
import datetime as dt
from decimal import ROUND_UP,Context,ROUND_DOWN
import requests
from urllib.parse import urlencode
import hashlib
import hmac

main_path_data = os.path.abspath(r"C:/inetpub/wwwroot/Arbitrage/data")
a_file1 = open(main_path_data + "\\rools.json", "r")
rools = json.load(a_file1)
a_file1.close()



def bot_sendtext2(bot_message):
    ### Send text message
    bot_chatID = "494797976"
    bot_token = "1106019018:AAGboAxP5aa5_14IbSxxzLOz9PrbjkwALs8"
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    requests.get(send_text)
    return

########################     ALFA    ##########################
def alfa(val1, val2, price, amount):
    #####  direction  (buy  / sell)
    from time import time

    if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
        if val1 == 'USD' or val1 == 'USDT':
            direction = "buy"
            pass
        else:
            direction = "sell"
            pass
    elif val1 != 'USD' and val2 != 'USD' and val1 != 'USDT' and val2 != 'USDT':
        if val1 == 'BTC':
            direction = "buy"
            pass
        else:
            direction = "sell"
            pass



    tickers_all = ['BTC_USD', 'PZM_USD', 'ETH_USD', 'ETH_USDT', 'PZM_BTC', 'ETH_BTC']

    parametr1 = "{}_{}".format(val1, val2)
    parametr2 = "{}_{}".format(val2, val1)

    for i in tickers_all:
        if i == parametr1:
            para = i
            pass
        elif i == parametr2:
            para = i
            pass

    # urll = 'https://btc-alpha.com/api/v1/pairs/'
    # respo = requests.request("GET", urll)
    # examm = respo.json()
    #
    # dec_val = para.split('_')
    #
    # for i in examm:
    #     if i['currency2'] == dec_val[1] and i['currency1'] == dec_val[0]:
    #         # amount = round(float(amount), int(i['price_precision']))
    #         amount = Context(prec=(i['price_precision'] + 2), rounding=ROUND_DOWN).create_decimal(amount)
    #         amount = float(amount)
    #         pass
    #     else:
    #         pass
    for i in rools['alfa']['amount_precision']:
        if para == i:

            print('AMOUNT 1 ####', amount)

            d = int(rools['alfa']['amount_precision'][i])

            def custom_round(number, ndigits=d):
                return int(number * 10 ** ndigits) / 10.0 ** ndigits if ndigits else int(number)

            amount = custom_round(amount)
            print('AMOUNT 3 ####', amount)
            pass
        else:
            pass
    for i in rools['alfa']['price_precision']:
        if para == i:

            # # price = format(price, '.10f')
            # print('PRICE  ####', price)
            # price = Context(prec=(rools['alfa']['price_precision'][i] + 1), rounding=ROUND_DOWN).create_decimal(price)
            # price = float(price)
            # print('PRICE  ####', price)

            print('PRICE  before ####', price)
            d = rools['alfa']['price_precision'][i]

            def custom_round(number, ndigits=d):
                return int(number * 10 ** ndigits) / 10.0 ** ndigits if ndigits else int(number)

            price = custom_round(price)
            print('PRICE after ####', price)
            pass
        else:
            pass


    def keys():
        if os.path.isfile(main_path_data + "\\keys.json"):
            pass
        else:

            dictionary = {"1": {"key": "Api key",
                                "api": "Api secret"},
                          "2": {"key": "Api key",
                                "api": "Api secret"},
                          "3": {"key": "Api key",
                                "api": "Api secret"},
                          "4": {"key": "Chat id", "api": "Token"}}

            keys1 = json.dumps(dictionary, indent=4)
            with open(main_path_data + "\\keys.json", "w") as outfile:
                outfile.write(keys1)
                outfile.close()
                pass

    keys()

    a_file = open(main_path_data + "\\keys.json", "r")
    json_object = json.load(a_file)
    a_file.close()

    input1 = json_object["1"]['key']
    input2 = json_object["1"]['api']

    if input1 != "Api key" and input2 != "Api secret":
        # Свой класс исключений
        class ScriptError(Exception):
            pass

        class ScriptQuitCondition(Exception):
            pass

        print('NEW ORDER :', 'ALFA', '\n')
        print('direction  :', direction)
        print('para  :', para)
        print('amount  :', amount)
        print('price  :', price)

        order = {
            'type': direction,
            'pair': para,
            'amount': str(amount),
            'price': price
        }

        def get_auth_headers(self, data):
            msg = input1 + urlencode(sorted(data.items(), key=lambda val: val[0]))
            sign = hmac.new(input2.encode(), msg.encode(), digestmod='sha256').hexdigest()

            return {
                'X-KEY': input1,
                'X-SIGN': sign,
                'X-NONCE': str(int(time() * 1000)),
            }

        response = requests.post('https://btc-alpha.com/api/v1/order/', data=order, headers=get_auth_headers({}, order))

        def resm():
            try:
                # Полученный ответ переводим в строку UTF, и пытаемся преобразовать из текста в объект Python
                obj = json.loads(response.text)
                # Смотрим, есть ли в полученном объекте ключ "error"
                if 'error' in obj and obj['error']:
                    # nl = '\n'
                    # bot_sendtext2(
                    #     f" BIRGA ALFA -: {nl} {obj} {nl} {order}")
                    return obj['error']
                    # Если есть, выдать ошибку, код дальше выполняться не будет
                    # raise ScriptError(obj['error'])
                # Вернуть полученный объект как результат работы ф-ции
                # nl = '\n'
                # bot_sendtext2(
                #     f" BIRGA ALFA +: {nl} {obj} {nl} {order}")
                try:
                    gg = obj['success']
                except:
                    gg = obj
                return gg
            except ValueError:
                # Если не удалось перевести полученный ответ (вернулся не JSON)
                return ScriptError('Ошибка анализа возвращаемых данных, получена строка', response)
                # raise ScriptError('Ошибка анализа возвращаемых данных, получена строка', response)

        return resm()

    else:
        return ["ОШИБКА"]

########################     HOT    ##########################
def hot(val1, val2, price, amount):
  #####  direction  (buy  / sell)

  if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
      if val1 == 'USD' or val1 == 'USDT':
          direction = 2
          pass
      else:
          direction = 1
          pass
  elif val1 != 'USD' and val2 != 'USD' and val1 != 'USDT' and val2 != 'USDT':
      if val1 == 'BTC':
          direction = 2
          pass
      else:
          direction = 1
          pass

  tickers_all = ['BTC/USD', 'BTC/USDT', 'PZM/USDT', 'ETH/USD', 'ETH/USDT', 'PZM/BTC', 'ETH/BTC']

  parametr1 = "{}/{}".format(val1, val2)
  parametr2 = "{}/{}".format(val2, val1)

  for i in tickers_all:
    if i == parametr1:
      para = i
      pass
    elif i == parametr2:
      para = i
      pass

  # print('PARA :', para)

  # urll = 'https://api.hotbit.io/api/v1/market.list'
  # respo = requests.request("GET", urll)
  # examm = respo.json()


  # dec_val = para.replace('/', '')
  #
  # for i in examm['result']:
  #     if i['name'] == dec_val:
  #         # print(i['stock_prec']+2)
  #         # print(type(i['stock_prec']))
  #         amount = Context(prec=(i['stock_prec']+2), rounding=ROUND_DOWN).create_decimal(amount)
  #         amount = float(amount)
  #         # print(amount)
  #         # amount = round(float(amount), int(i['stock_prec']))
  #         pass
  #     else:
  #         pass
  for i in rools['hot']['amount_precision']:
        if para == i:
            print('AMOUNT 1 ####', amount)

            d = int(rools['hot']['amount_precision'][i])

            def custom_round(number, ndigits=d):
                return int(number * 10 ** ndigits) / 10.0 ** ndigits if ndigits else int(number)

            amount = custom_round(amount)
            print('AMOUNT 3 ####', amount)

            pass
        else:
            pass
  for i in rools['hot']['price_precision']:
        if para == i:

            print('PRICE  before ####', price)
            d = rools['hot']['price_precision'][i]

            def custom_round(number, ndigits=d):
                return int(number * 10 ** ndigits) / 10.0 ** ndigits if ndigits else int(number)

            price = custom_round(price)
            print('PRICE after ####', price)
            pass
        else:
            pass

  def keys():
    if os.path.isfile(main_path_data + "\\keys.json"):
      pass
    else:

      dictionary = {"1": {"key": "Api key",
                          "api": "Api secret"},
                    "2": {"key": "Api key",
                          "api": "Api secret"},
                    "3": {"key": "Api key",
                          "api": "Api secret"},
                    "4": {"key": "Chat id", "api": "Token"}}

      keys1 = json.dumps(dictionary, indent=4)
      with open(main_path_data + "\\keys.json", "w") as outfile:
        outfile.write(keys1)
        outfile.close()
        pass
  keys()

  a_file = open(main_path_data + "\\keys.json", "r")
  json_object = json.load(a_file)
  a_file.close()

  input1 = json_object["3"]['key']
  input2 = json_object["3"]['api']


  if input1 != "Api key" and input2 != "Api secret":
      # Свой класс исключений
      class ScriptError(Exception):
          pass

      class ScriptQuitCondition(Exception):
          pass

      print('\n', 'NEW ORDER :', 'HOT', '\n')
      print('direction  :', direction)
      print('para  :', para)
      print('amount  :', amount)
      print('price  :', price)

      msg = "amount={}&api_key={}&isfee=0&market={}&price={}&side={}&secret_key={}".format(
          amount, input1, para, price, direction, input2)

      # print("####   MSG  :",msg)
      sign = hashlib.md5(msg.encode()).hexdigest().upper()
      url2 = 'https://api.hotbit.io/api/v1/order.put_limit?amount={}&api_key={}&isfee=0&market={}&price={}&side={}&sign={}'.format(amount, input1, para, price, direction,sign)

      # print("####   url2  :", url2)

      response = requests.request("GET", url2)
      # exam2 = response.json()
      def resm():
        try:
          # Полученный ответ переводим в строку UTF, и пытаемся преобразовать из текста в объект Python
          obj = json.loads(response.text)
          # print('obj :', obj)
          # Смотрим, есть ли в полученном объекте ключ "error"
          if 'error' in obj and obj['error']:
            # nl = '\n'
            # bot_sendtext2(f" BIRGA HOT -: {nl} {obj} {nl} {order}")
            return obj['error']['message']
            # Если есть, выдать ошибку, код дальше выполняться не будет
            # raise ScriptError(obj['error'])
          # Вернуть полученный объект как результат работы ф-ции
          # nl = '\n'
          # bot_sendtext2(
          #     f" BIRGA HOT +: {nl} {obj}")
          return obj['result']['id']
        except ValueError:
          # Если не удалось перевести полученный ответ (вернулся не JSON)
          return ScriptError('Ошибка анализа возвращаемых данных, получена строка', response)
          # raise ScriptError('Ошибка анализа возвращаемых данных, получена строка', response)

      return resm()


  else:
    return ["ОШИБКА"]

#######################     Live    ##########################
def live(val1, val2, price, amount):
    #####  direction  (buy  / sell)
    if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
        if val1 == 'USD' or val1 == 'USDT':
            direction = "buy"
            pass
        else:
            direction = "sell"
            pass
    elif val1 != 'USD' and val2 != 'USD' and val1 != 'USDT' and val2 != 'USDT':
        if val1 == 'BTC':
            direction = "buy"
            pass
        else:
            direction = "sell"
            pass

    tickers_all = ['BTC/USD', 'PZM/USD', 'PZM/USDT', 'ETH/USD', 'ETH/USDT', 'PZM/BTC', 'ETH/BTC']

    parametr1 = "{}/{}".format(val1, val2)
    parametr2 = "{}/{}".format(val2, val1)

    for i in tickers_all:
        if i == parametr1:
            para = i
            pass
        elif i == parametr2:
            para = i
            pass

    # url = 'https://api.livecoin.net/exchange/restrictions'
    # respo = requests.request("GET", url)
    # examm = respo.json()
    #
    # for i in examm['restrictions']:
    #     if i['currencyPair'] == para:
    #         # amount = round(float(amount), int(i['priceScale']))
    #         amount = Context(prec=(i['priceScale'] + 1), rounding=ROUND_DOWN).create_decimal(amount)
    #         amount = float(amount)
    #         pass
    #     else:
    #         pass
    for i in rools['live']['amount_precision']:
        if para == i:
            print('AMOUNT  ####', amount)
            d = int(rools['live']['amount_precision'][i])
            def custom_round(number, ndigits=d):
                return int(number * 10 ** ndigits) / 10.0 ** ndigits if ndigits else int(number)

            amount = custom_round(amount)
            print('AMOUNT  ####', amount)
            pass
        else:
            pass
    for i in rools['live']['price_precision']:
        if para == i:
            # price = format(price, '.10f')
            print('PRICE  ####', price)
            # price = Context(prec=(rools['live']['price_precision'][i] + 1), rounding=ROUND_DOWN).create_decimal(price)
            # price = float(price)

            d = rools['live']['price_precision'][i]

            def custom_round(number, ndigits=d):
                return int(number * 10 ** ndigits) / 10.0 ** ndigits if ndigits else int(number)

            price = custom_round(price)
            print('PRICE  ####', price)
            pass
        else:
            pass

    def keys():
        if os.path.isfile(main_path_data + "\\keys.json"):
            pass
        else:

            dictionary = {"1": {"key": "Api key",
                                "api": "Api secret"},
                          "2": {"key": "Api key",
                                "api": "Api secret"},
                          "3": {"key": "Api key",
                                "api": "Api secret"},
                          "4": {"key": "Chat id", "api": "Token"}}

            keys1 = json.dumps(dictionary, indent=4)
            with open(main_path_data + "\\keys.json", "w") as outfile:
                outfile.write(keys1)
                outfile.close()
                pass

    keys()

    a_file = open(main_path_data + "\\keys.json", "r")
    json_object = json.load(a_file)
    a_file.close()

    input1 = json_object["2"]['key']
    input2 = json_object["2"]['api']


    if input1 != "Api key" and input2 != "Api secret":
        # Свой класс исключений
        class ScriptError(Exception):
            pass

        class ScriptQuitCondition(Exception):
            pass

        print('\n', '----NEW ORDER :', 'LIVE-----', '\n')
        print('direction  :', direction)
        print('para  :', para)
        print('amount  :', amount)
        print('price  :', price)

        order = {
            'currencyPair': para,
            'quantity': str(amount),
            'price': price
        }
        order2 = urlencode(sorted(order.items(), key=lambda val: val[0]))

        def get_auth_headers(self, data):
            # msg = input1 + urlencode(sorted(data.items(), key=lambda val: val[0]))
            msg = urlencode(sorted(data.items(), key=lambda val: val[0]))
            sign = hmac.new(input2.encode(), msg=msg.encode(), digestmod='sha256').hexdigest().upper()

            return {
                'Api-key': input1,
                'Sign': sign,
                "Content-type": "application/x-www-form-urlencoded"
            }


        if direction == 'sell':
            response = requests.post('https://api.livecoin.net/exchange/selllimit', data=order2,
                                     headers=get_auth_headers({}, order))
        #     /exchange/selllimit   /exchange/buylimit

        else:
            response = requests.post('https://api.livecoin.net/exchange/buylimit', data=order2,
                                     headers=get_auth_headers({}, order))

        def resm():
            try:
                # Полученный ответ переводим в строку UTF, и пытаемся преобразовать из текста в объект Python
                obj = json.loads(response.text)

                # print("1", obj)
                # print("2", obj['success'])

                # Смотрим, есть ли в полученном объекте ключ "error"
                if obj['success'] == False:
                    # nl = '\n'
                    # bot_sendtext2(
                    #     f" BIRGA LIVE -: {nl} {obj} {nl} {order}")
                    return obj['exception']
                    # Если есть, выдать ошибку, код дальше выполняться не будет
                    # raise ScriptError(obj['error'])
                # Вернуть полученный объект как результат работы ф-ции
                # nl = '\n'
                # bot_sendtext2(
                #     f" BIRGA LIVE +: {nl} {obj} {nl} {order}")
                return obj['success']
            except ValueError:
                # Если не удалось перевести полученный ответ (вернулся не JSON)
                return ScriptError('Ошибка анализа возвращаемых данных, получена строка', response)
                # raise ScriptError('Ошибка анализа возвращаемых данных, получена строка', response)

        return resm()

    else:
        return ["ОШИБКА"]


def bot_sendtext(bot_message):
    ##########################    Telegram    ################################

    ad = open(main_path_data + "\\keys.json", "r")
    js_object = json.load(ad)
    ad.close()
    input1 = js_object["4"]['key']
    input2 = js_object["4"]['api']

    ### Send text message
    bot_token = input1
    bot_chatID = input2
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    requests.get(send_text)
    return
def all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol, reponse_b1, reponse_b2, start11):
    ###################    APPEND to CSV   all_data    #####################
    import time
    if reponse_b1 != 'Not Enough Money' or reponse_b2 != 'Not Enough Money':
        done = int(round(time.time() * 1000))
        now = dt.datetime.now()
        df_all = pd.read_csv(main_path_data + "\\all_data.csv")
        timer2 = now.strftime("%H:%M:%S")
        df2 = pd.DataFrame({"TIME": [timer2],
                            "birga_x": [birga_1],
                            "birga_y": [birga_2],
                            "rates_x": [rate1],
                            "rates_y": [rate2],
                            "valin_x": [val1],
                            "valin_y": [val2],
                            "valout_y": [val4],
                            "start": [val1_vol],
                            "step": [val2_vol],
                            "back": [val4_vol],
                            "profit": [(float(val4_vol) - float(val1_vol))],
                            "perc": [(((float(val4_vol) - float(val1_vol)) / float(val1_vol)) * 100)],
                            "res_birga1": [reponse_b1],
                            "res_birga2": [reponse_b2],
                            "timer": [done],
                            }, index=[0])
        df_all = df2.append(df_all)
        df_all.to_csv(main_path_data + "\\all_data.csv", header=True, index=False)

        ##########           Change CSV BALANCE        ###################
        valuta = pd.read_csv(main_path_data + "\\balance.csv")
        valuta[birga_1] = valuta[birga_1].apply(pd.to_numeric, errors='coerce')
        valuta[birga_2] = valuta[birga_2].apply(pd.to_numeric, errors='coerce')

        filter1 = valuta[valuta['Valuta'] == val1].index
        filter3 = valuta[valuta['Valuta'] == val2].index
        valuta.loc[filter1, birga_1] = valuta.loc[filter1, birga_1] - float(val1_vol) - ((float(val1_vol) * 1) / 100)
        valuta.loc[filter3, birga_2] = valuta.loc[filter3, birga_2] - float(val2_vol) - ((float(val2_vol) * 1) / 100)

        fil1 = valuta[valuta['Valuta'] == 'USD']
        fil2 = valuta[valuta['Valuta'] == 'USDT']

        al = (float(fil1.iloc[0]['alfa']) + float(fil2.iloc[0]['alfa']))
        li = (float(fil1.iloc[0]['live']) + float(fil2.iloc[0]['live']))
        ho = (float(fil1.iloc[0]['hot']) + float(fil2.iloc[0]['hot']))

        filter = valuta[valuta['Valuta'] == 'USDT + USD'].index
        valuta.loc[filter, "alfa"] = al
        valuta.loc[filter, "live"] = li
        valuta.loc[filter, "hot"] = ho

        valuta.loc[:, "Summa"] = (valuta.loc[:, "alfa"] + valuta.loc[:, "live"] + valuta.loc[:, "hot"])
        valuta.to_csv(main_path_data + "\\balance.csv", index=False)

        #################    TELEGRAM    #########################
        profit = (float(val4_vol) - float(val1_vol))
        perc = (((float(val4_vol) - float(val1_vol)) / float(val1_vol)) * 100)
        nl = '\n'
        val1_vol = ("{:.6f}".format(val1_vol))
        val2_vol = ("{:.6f}".format(val2_vol))
        val4_vol = ("{:.6f}".format(val4_vol))
        profit = ("{:.6f}".format(profit))
        perc = ("{:.2f}".format(perc))
        bot_sendtext(
            f" ЕСТЬ ВИЛКА: {nl} {birga_1} / {birga_2} {nl} {reponse_b1} / {reponse_b2} {nl} {val1} -> {val2} -> {val4} {nl} {val1_vol} -> {val2_vol} -> {val4_vol} {nl} {profit} {nl} {perc} {nl} ")

        print('---------   OUT  from CSV  --------')
        return
    else:
        pass
    return
def order(regims, birga_1, birga_2, val1_vol, val1, rate1, val2_vol, val2, val3_vol, val3, rate2, val4_vol, val4, valuta, start11, first):
    while True:

        print('---------   START of ORDER  --------')
        filter1 = valuta[valuta['Valuta'] == val1]
        filter3 = valuta[valuta['Valuta'] == val3]

        if filter1.shape[0] < 1:
            dict = {'Valuta': val1, 'alfa': 0, 'live': 0, 'hot': 0}
            filter1 = pd.DataFrame([dict])
            pass
        else:
            pass
        if filter3.shape[0] < 1:
            dict = {'Valuta': val3, 'alfa': 0, 'live': 0, 'hot': 0}
            filter3 = pd.DataFrame([dict])
            pass
        else:
            pass

        a_file = open(main_path_data + "\\regims2.json", "r")
        regim = json.load(a_file)
        a_file.close()
        parametr1 = "{}/{}".format(val1, val2)
        para1 = ['BTC/USD', 'LTC/USD', 'ETH/USD', 'XRP/USD', 'USDT/USD', 'BTC/USDT', 'ETH/USDT', 'XRP/BTC', 'ETH/BTC',
                 'LTC/BTC', 'BCH/BTC', 'ZEC/BTC', 'PZM/USD', 'PZM/USDT', 'PZM/BTC']


        for i in para1:
            if i == parametr1:
                parad = "ok"
                break
            else:
                parad = "no"
                break
        print('---------   parad  ok/no  --------')

        if parad == 'ok':
            print('---------   OK   --------')
            kurs = (float(rate1) / float(val1_vol))
            kurs2 = (float(val3_vol) / float(val4_vol))
            kurs0 = (float(val2_vol) / float(val1_vol))
            minA = regim[str(regims)]["order"]
            minB = minA * kurs0

            minbeta = (((float(val1_vol) - float(val2_vol) * float(rate1)) / (
                    float(val2_vol) * float(rate1))) * 100)
            minbeta = Context(prec=3, rounding=ROUND_UP).create_decimal(minbeta)
            minbeta = float(minbeta)



            if filter1.iloc[0][birga_1] > float(val1_vol) and filter3.iloc[0][birga_2] > float(val3_vol):
                print('---------   OK 1  --------')
                if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
                    if birga_1 == 'alfa' and birga_2 == 'live':
                        if val2 != 'USD' and val2 != 'USDT':
                            val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                        else:
                            reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                            reponse_b2 = live(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'live' and birga_2 == 'alfa':
                        if val2 != 'USD' and val2 != 'USDT':
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                        else:
                            val4_vol = val4_vol + (val4_vol * minbeta / 100)
                            reponse_b1 = live(val1, val2, rate1, val1_vol)
                            reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'alfa' and birga_2 == 'hot':
                        if val2 != 'USD' and val2 != 'USDT':
                            val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break


                        else:
                            reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                            reponse_b2 = hot(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'hot' and birga_2 == 'alfa':
                        if val2 != 'USD' and val2 != 'USDT':
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break


                        else:
                            val4_vol = val4_vol + (val4_vol * minbeta / 100)
                            reponse_b1 = hot(val1, val2, rate1, val1_vol)
                            reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'hot' and birga_2 == 'live':
                        if val2 != 'USD' and val2 != 'USDT':
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break



                        else:
                            reponse_b1 = hot(val1, val2, rate1, val1_vol)
                            reponse_b2 = live(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'live' and birga_2 == 'hot':
                        if val2 != 'USD' and val2 != 'USDT':
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                        else:
                            reponse_b1 = live(val1, val2, rate1, val1_vol)
                            reponse_b2 = hot(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    else:
                        break
                elif val1 != 'USD' or val1 != 'USDT' or val2 != 'USD' and val2 != 'USDT':
                    if birga_1 == 'alfa' and birga_2 == 'live':
                        if val2 != "BTC":
                            val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break


                        else:
                            reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                            reponse_b2 = live(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'live' and birga_2 == 'alfa':
                        if val2 != "BTC":
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break


                        else:
                            val4_vol = val4_vol + (val4_vol * minbeta / 100)
                            reponse_b1 = live(val1, val2, rate1, val1_vol)
                            reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'alfa' and birga_2 == 'hot':
                        if val2 != "BTC":
                            val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break


                        else:
                            reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                            reponse_b2 = hot(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'hot' and birga_2 == 'alfa':
                        if val2 != "BTC":
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break


                        else:
                            val4_vol = val4_vol + (val4_vol * minbeta / 100)
                            reponse_b1 = hot(val1, val2, rate1, val1_vol)
                            reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'hot' and birga_2 == 'live':
                        if val2 != "BTC":
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            reponse_b1 = hot(val1, val2, rate1, val1_vol)
                            reponse_b2 = live(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'live' and birga_2 == 'hot':
                        if val2 != "BTC":
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break


                        else:
                            reponse_b1 = live(val1, val2, rate1, val1_vol)
                            reponse_b2 = hot(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    else:
                        break




            elif filter1.iloc[0][birga_1] < float(val1_vol) and filter3.iloc[0][birga_2] > minB and filter1.iloc[0][
                birga_1] > minA or filter3.iloc[0][birga_2] < float(val3_vol) and filter3.iloc[0][birga_2] > minB and \
                    filter1.iloc[0][birga_1] > minA:
                print('---------   OK 2  --------')

                minOrder1 = float(filter1.iloc[0][birga_1] * kurs)
                minOrder2 = float(filter3.iloc[0][birga_2])
                if minOrder2 > minOrder1:
                    val1_vol = filter1.iloc[0][birga_1]
                    val2_vol = minOrder1
                    val3_vol = minOrder1
                    val4_vol = minOrder1 / kurs2


                    if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
                        if birga_1 == 'alfa' and birga_2 == 'live':
                            if val2 != 'USD' and val2 != 'USDT':
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break


                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'alfa':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'alfa' and birga_2 == 'hot':
                            if val2 != 'USD' and val2 != 'USDT':
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'alfa':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'live':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'hot':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        else:
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                            break
                    elif val1 != 'USD' or val1 != 'USDT' or val2 != 'USD' and val2 != 'USDT':
                        if birga_1 == 'alfa' and birga_2 == 'live':
                            if val2 != "BTC":
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'alfa':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'alfa' and birga_2 == 'hot':
                            if val2 != "BTC":
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'alfa':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'live':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'hot':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        else:
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val3_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                            break
                elif minOrder2 < minOrder1:
                    val1_vol = minOrder2 / kurs
                    val2_vol = minOrder2
                    val3_vol = minOrder2
                    val4_vol = minOrder2 / kurs2


                    if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
                        if birga_1 == 'alfa' and birga_2 == 'live':
                            if val2 != 'USD' and val2 != 'USDT':
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'alfa':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'alfa' and birga_2 == 'hot':
                            if val2 != 'USD' and val2 != 'USDT':
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'alfa':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'live':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'hot':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        else:
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                            break
                    elif val1 != 'USD' or val1 != 'USDT' or val2 != 'USD' and val2 != 'USDT':
                        if birga_1 == 'alfa' and birga_2 == 'live':
                            if val2 != "BTC":
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'alfa':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'alfa' and birga_2 == 'hot':
                            if val2 != "BTC":
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'alfa':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'live':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'hot':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        else:
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                            break
                else:
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                        "Not Enough Money", "Not Enough Money", start11)
                    break


            elif filter1.iloc[0][birga_1] < minA or filter3.iloc[0][birga_2] < minB:
                print('---------   OK 3  --------')
                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                        "Not Enough Money", "Not Enough Money", start11)
                break
            else:
                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                        "Not Enough Money", "Not Enough Money", start11)
                break
        elif parad == 'no':
            print('---------   parad NO --------')
            kurs = (float(val1_vol) * float(val2_vol))
            kurs2 = (float(val4_vol) / float(val3_vol))
            kurs0 = (float(val1_vol) / float(val2_vol))
            minB = regim[str(regims)]["order"]
            minA = minB * kurs0

            minbeta = (((float(val1_vol) - float(val2_vol) * float(rate1)) / (
                    float(val2_vol) * float(rate1))) * 100)
            minbeta = Context(prec=3, rounding=ROUND_UP).create_decimal(minbeta)
            minbeta = float(minbeta)
            min1 = (float(filter1.iloc[0][birga_1]) - (float(filter1.iloc[0][birga_1]) * minbeta / 100)) / float(rate1)
            min2 = float(filter3.iloc[0][birga_2])


            if filter1.iloc[0][birga_1] > float(val1_vol) and filter3.iloc[0][birga_2] > float(val3_vol):
                print('---------   NO  #1  --------')
                if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
                    if birga_1 == 'alfa' and birga_2 == 'live':
                        print('---------   ALFA  / LIVE   1  --------')
                        if val2 != 'USD' and val2 != 'USDT':
                            print('---------   ALFA  / LIVE   21  --------')
                            val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                print('---------   ALFA  / LIVE   EXIT 211  --------')
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                print('---------   ALFA  / LIVE   EXIT 212  --------')
                                break

                        else:
                            print('---------   ALFA  / LIVE   22  --------')
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)

                                print('---------   ALFA  / LIVE   EXIT 22  --------')
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                print('---------   ALFA  / LIVE   EXIT 222  --------')
                                break
                    elif birga_1 == 'live' and birga_2 == 'alfa':
                        if val2 != 'USD' and val2 != 'USDT':
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            val4_vol = val4_vol + (val4_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                    elif birga_1 == 'alfa' and birga_2 == 'hot':
                        if val2 != 'USD' and val2 != 'USDT':
                            val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                    elif birga_1 == 'hot' and birga_2 == 'alfa':
                        if val2 != 'USD' and val2 != 'USDT':
                            # val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            val4_vol = val4_vol + (val4_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                    elif birga_1 == 'hot' and birga_2 == 'live':
                        if val2 != 'USD' or 'USDT':
                            # val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                    elif birga_1 == 'live' and birga_2 == 'hot':
                        if val2 != 'USD' and val2 != 'USDT':
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                    else:
                        all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                        break
                elif val1 != 'USD' or val1 != 'USDT' or val2 != 'USD' or val2 != 'USDT':
                    if birga_1 == 'alfa' and birga_2 == 'live':
                        if val2 != "BTC":
                            val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                            reponse_b2 = live(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'live' and birga_2 == 'alfa':
                        if val2 != "BTC":
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            val4_vol = val4_vol + (val4_vol * minbeta / 100)
                            reponse_b1 = live(val1, val2, rate1, val1_vol)
                            reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'alfa' and birga_2 == 'hot':
                        if val2 != "BTC":
                            val2_vol = val2_vol + (val2_vol * minbeta / 100)
                            if first == '1':
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                            reponse_b2 = hot(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'hot' and birga_2 == 'alfa':
                        if val2 != "BTC":
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            val4_vol = val4_vol + (val4_vol * minbeta / 100)
                            reponse_b1 = hot(val1, val2, rate1, val1_vol)
                            reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'hot' and birga_2 == 'live':
                        if val2 != "BTC":
                            if first == '1':
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = live(val3, val4, rate2, val3_vol)
                                reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break

                        else:
                            reponse_b1 = hot(val1, val2, rate1, val1_vol)
                            reponse_b2 = live(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    elif birga_1 == 'live' and birga_2 == 'hot':
                        if val2 != "BTC":
                            if first == '1':
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break
                            else:
                                reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                reponse_b1 = live(val1, val2, rate1, val2_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                break


                        else:
                            reponse_b1 = live(val1, val2, rate1, val1_vol)
                            reponse_b2 = hot(val3, val4, rate2, val4_vol)
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                            break
                    else:
                        all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                        break
            elif filter1.iloc[0][birga_1] < float(val1_vol) and filter3.iloc[0][birga_2] > minB and filter1.iloc[0][
                birga_1] > minA or filter3.iloc[0][birga_2] < float(val3_vol) and filter3.iloc[0][birga_2] > minB and \
                    filter1.iloc[0][birga_1] > minA:
                print('---------   NO  #2  --------')
                if min2 > min1:
                    val1_vol = filter1.iloc[0][birga_1]
                    val2_vol = min1
                    val3_vol = min1
                    val4_vol = min1 * kurs2

                    if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
                        if birga_1 == 'alfa' and birga_2 == 'live':
                            if val2 != 'USD' and val2 != 'USDT':
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'alfa':
                            val3_vol = min1 - (min1 * minbeta / 100)
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'alfa' and birga_2 == 'hot':
                            if val2 != 'USD' and val2 != 'USDT':
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'alfa':
                            val3_vol = min1 - (min1 * minbeta / 100)
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'live':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'hot':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        else:
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                    val4_vol,
                                "No Such Command", "No Such Command", start11)
                            break
                    elif val1 != 'USD' or val1 != 'USDT' or val2 != 'USD' or val2 != 'USDT':
                        if birga_1 == 'alfa' and birga_2 == 'live':
                            if val2 != "BTC":
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'alfa':
                            val3_vol = min1 - (min1 * minbeta / 100)
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'alfa' and birga_2 == 'hot':
                            if val2 != "BTC":
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'alfa':
                            val3_vol = min1 - (min1 * minbeta / 100)
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                            val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol,
                                        val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'live':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break
                            else:
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'hot':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break
                            else:
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        else:
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                            break
                elif min2 < min1:
                    val1_vol = (float(filter3.iloc[0][birga_2]) + (
                            float(filter3.iloc[0][birga_2]) * minbeta / 100)) * float(rate1)
                    val2_vol = float(filter3.iloc[0][birga_2])
                    val3_vol = float(filter3.iloc[0][birga_2])
                    val4_vol = float(filter3.iloc[0][birga_2]) * kurs2

                    if val1 == 'USD' or val1 == 'USDT' or val2 == 'USD' or val2 == 'USDT':
                        if birga_1 == 'alfa' and birga_2 == 'live':
                            if val2 != 'USD' and val2 != 'USDT':
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'alfa':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'alfa' and birga_2 == 'hot':
                            if val2 != 'USD' and val2 != 'USDT':
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'alfa':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break
                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'live':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break
                            else:
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'hot':
                            if val2 != 'USD' and val2 != 'USDT':
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break
                            else:
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        else:
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                            break
                    elif val1 != 'USD' or val1 != 'USDT' or val2 != 'USD' or val2 != 'USDT':
                        if birga_1 == 'alfa' and birga_2 == 'live':
                            if val2 != "BTC":
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break
                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'alfa':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break
                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'alfa' and birga_2 == 'hot':
                            if val2 != "BTC":
                                val2_vol = val2_vol + (val2_vol * minbeta / 100)
                                if first == '1':
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break
                            else:
                                reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'alfa':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                val4_vol = val4_vol + (val4_vol * minbeta / 100)
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'hot' and birga_2 == 'live':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = hot(val1, val2, rate1, val1_vol)
                                reponse_b2 = live(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        elif birga_1 == 'live' and birga_2 == 'hot':
                            if val2 != "BTC":
                                if first == '1':
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                        reponse_b1, reponse_b2, start11)
                                    break
                                else:
                                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                            reponse_b1, reponse_b2, start11)
                                    break

                            else:
                                reponse_b1 = live(val1, val2, rate1, val1_vol)
                                reponse_b2 = hot(val3, val4, rate2, val4_vol)
                                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                    reponse_b1, reponse_b2, start11)
                                break
                        else:
                            all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                            break
                else:
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                                "No Such Command", "No Such Command", start11)
                    break
            elif filter1.iloc[0][birga_1] < minA or filter3.iloc[0][birga_2] < minB:
                print('---------   NO  #3  --------')
                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                        "Not Enough Money", "Not Enough Money", start11)
                break
            else:
                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                        "Not Enough Money", "Not Enough Money", start11)
                break
    print('---------   ORDER  EXIT   --------')
    return
def avtomat(dft, valuta, start11):
    print('---------   START AVTOMAT  --------')
    all_cardsBD = dft
    all_cardsBD.index += 1
    all_cardsBD['start'] = all_cardsBD['start'].apply(pd.to_numeric, errors='coerce')
    all_cardsBD['volume'] = all_cardsBD['volume'].apply(pd.to_numeric, errors='coerce')
    all_cardsBD['back'] = all_cardsBD['back'].apply(pd.to_numeric, errors='coerce')
    all_cardsBD['step'] = all_cardsBD['step'].apply(pd.to_numeric, errors='coerce')
    all_cardsBD['volume_x'] = all_cardsBD['volume_x'].apply(pd.to_numeric, errors='coerce')
    all_cardsBD['volume_y'] = all_cardsBD['volume_y'].apply(pd.to_numeric, errors='coerce')
    all_cardsBD["volume_x"] = all_cardsBD["volume_x"].astype(float)
    all_cardsBD["volume_y"] = all_cardsBD["volume_y"].astype(float)

    filterx = all_cardsBD[all_cardsBD["volume_x"] < all_cardsBD["volume_y"]].index
    all_cardsBD.loc[filterx, "first"] = '1'
    filtery = all_cardsBD[all_cardsBD["volume_x"] > all_cardsBD["volume_y"]].index
    all_cardsBD.loc[filtery, "first"] = '2'

    USD_fil = all_cardsBD[(all_cardsBD['valin_x'] == "USD") | (all_cardsBD['valin_x'] == "USDT")]
    BTC_fil = all_cardsBD[(all_cardsBD['valin_x'] == "BTC") & (all_cardsBD['valin_y'].isin(["USD", "USDT"]))]
    BTC_fil_main = all_cardsBD[(all_cardsBD['valin_x'] == "BTC") & (~all_cardsBD['valin_y'].isin(["USD", "USDT"]))]
    BTC_fil_main2 = all_cardsBD[(all_cardsBD['valin_y'] == "BTC") & (~all_cardsBD['valin_x'].isin(["USD", "USDT"]))]

    if USD_fil.shape[0]>0:
        for ind in USD_fil.index:
            rate11 = (USD_fil['start'][ind] / USD_fil['step'][ind])
            rate22 = (USD_fil['back'][ind] / USD_fil['step'][ind])
            step = (USD_fil['volume'][ind] * rate11)
            step2 = (rate22 * USD_fil['volume'][ind])
            regims = USD_fil['regim'][ind]
            birga_1 = USD_fil['birga_x'][ind]
            birga_2 = USD_fil['birga_y'][ind]
            val1_vol = step
            val1 = USD_fil['valin_x'][ind]
            rate1 = USD_fil['rates_x'][ind]
            val2_vol = USD_fil['volume'][ind]
            val2 = USD_fil['valin_y'][ind]
            val3_vol = USD_fil['volume'][ind]
            val3 = USD_fil['valin_y'][ind]
            rate2 = USD_fil['rates_y'][ind]
            val4_vol = step2
            val4 = USD_fil['valout_y'][ind]
            first = USD_fil['first'][ind]
            order(regims, birga_1, birga_2, val1_vol, val1, rate1, val2_vol, val2, val3_vol, val3, rate2, val4_vol, val4, valuta, start11, first)
            break
        print('---------   EXIT AVTOMAT USD --------')
        return
    elif BTC_fil.shape[0]>0:
        for ind in BTC_fil.index:
            rate11 = (BTC_fil['step'][ind] / BTC_fil['start'][ind])
            rate22 = (BTC_fil['step'][ind] / BTC_fil['back'][ind])
            step = (BTC_fil['volume'][ind] * rate11)
            step2 =(step / rate22)
            regims = BTC_fil['regim'][ind]
            birga_1 = BTC_fil['birga_x'][ind]
            birga_2 = BTC_fil['birga_y'][ind]
            val1_vol = BTC_fil['volume'][ind]
            val1 = BTC_fil['valin_x'][ind]
            rate1 = BTC_fil['rates_x'][ind]
            val2_vol = step
            val2 = BTC_fil['valin_y'][ind]
            val3_vol = step
            val3 = BTC_fil['valin_y'][ind]
            rate2 = BTC_fil['rates_y'][ind]
            val4_vol = step2
            val4 = BTC_fil['valout_y'][ind]
            first = BTC_fil['first'][ind]
            order(regims, birga_1, birga_2, val1_vol, val1, rate1, val2_vol, val2, val3_vol, val3, rate2, val4_vol,
                  val4, valuta, start11, first)
            break
        print('---------   EXIT AVTOMAT BTC --------')
        return
    elif BTC_fil_main.shape[0]>0:
        for ind in BTC_fil_main.index:
            rate11 = (BTC_fil_main['start'][ind] / BTC_fil_main['step'][ind])
            rate22 = (BTC_fil_main['back'][ind] / BTC_fil_main['step'][ind])
            step = (BTC_fil_main['volume'][ind] * rate11)
            step2 = (rate22 * BTC_fil_main['volume'][ind])
            regims = BTC_fil_main['regim'][ind]
            birga_1 = BTC_fil_main['birga_x'][ind]
            birga_2 = BTC_fil_main['birga_y'][ind]
            val1_vol = step
            val1 = BTC_fil_main['valin_x'][ind]
            rate1 = BTC_fil_main['rates_x'][ind]
            val2_vol = BTC_fil_main['volume'][ind]
            val2 = BTC_fil_main['valin_y'][ind]
            val3_vol = BTC_fil_main['volume'][ind]
            val3 = BTC_fil_main['valin_y'][ind]
            rate2 = BTC_fil_main['rates_y'][ind]
            val4_vol = step2
            val4 = BTC_fil_main['valout_y'][ind]
            first = BTC_fil_main['first'][ind]
            order(regims, birga_1, birga_2, val1_vol, val1, rate1, val2_vol, val2, val3_vol, val3, rate2, val4_vol,
                  val4, valuta, start11, first)
            break
        print('---------   EXIT AVTOMAT BTC2 --------')
        return
    elif BTC_fil_main2.shape[0]>0:
        for ind in BTC_fil_main2.index:
            rate11 = (BTC_fil_main2['step'][ind] / BTC_fil_main2['start'][ind])
            rate22 = (BTC_fil_main2['step'][ind] / BTC_fil_main2['back'][ind])
            step = (BTC_fil_main2['volume'][ind] * rate11)
            step2 =(step / rate22)
            regims = BTC_fil_main2['regim'][ind]
            birga_1 = BTC_fil_main2['birga_x'][ind]
            birga_2 = BTC_fil_main2['birga_y'][ind]
            val1_vol = BTC_fil_main2['volume'][ind]
            val1 = BTC_fil_main2['valin_x'][ind]
            rate1 = BTC_fil_main2['rates_x'][ind]
            val2_vol = step
            val2 = BTC_fil_main2['valin_y'][ind]
            val3_vol = step
            val3 = BTC_fil_main2['valin_y'][ind]
            rate2 = BTC_fil_main2['rates_y'][ind]
            val4_vol = step2
            val4 = BTC_fil_main2['valout_y'][ind]
            first = BTC_fil_main2['first'][ind]
            order(regims, birga_1, birga_2, val1_vol, val1, rate1, val2_vol, val2, val3_vol, val3, rate2, val4_vol,
                  val4, valuta, start11, first)
            break
        print('---------   EXIT AVTOMAT BTC22 --------')
        return
    else:
        print('---------   EXIT AVTOMAT --------')
        return
