import pandas as pd
import json
import os
import datetime as dt
import requests
from urllib.parse import urlencode
import hashlib
import hmac

main_path_data = os.path.abspath(r"C:/inetpub/wwwroot/WBW/data")
a_file1 = open(main_path_data + "\\rools.json", "r")
rools = json.load(a_file1)
a_file1.close()


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

            # print('PRICE  before ####', price)
            d = rools['alfa']['price_precision'][i]

            # def custom_round(number, ndigits=d):
            #     return int(number * 10 ** ndigits) / 10.0 ** ndigits if ndigits else int(number)
            #
            # price = custom_round(price)


            price = round(float(price), d)
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
                    gg = obj['oid']
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

  for i in rools['hot']['amount_precision']:
        if para == i:
            print('AMOUNT 1 ####', amount)

            d = int(rools['hot']['amount_precision'][i])

            def custom_round(number, ndigits=d):
                return int(number * 10 ** ndigits) / 10.0 ** ndigits if ndigits else int(number)

            amount = custom_round(float(price))
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

            price = custom_round(float(price))
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
                return obj['orderId']
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
def all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol, reponse_b1, reponse_b2, regim):
    ###################    APPEND to CSV   all_data    #####################
    import time
    if reponse_b1 != 'Not Enough Money' or reponse_b2 != 'Not Enough Money':
        done = int(round(time.time() * 1000))
        now = dt.datetime.now()
        df_all = pd.read_csv(main_path_data + "\\all_data.csv")
        timer2 = now.strftime("%Y-%m-%d %H:%M:%S")
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
            f" ЕСТЬ ВИЛКА: {nl} РЕЖИМ : {regim} {nl} {birga_1} / {birga_2} {nl} {reponse_b1} / {reponse_b2} {nl} {val1} -> {val2} -> {val4} {nl} {val1_vol} -> {val2_vol} -> {val4_vol} {nl} {profit} {nl} {perc} {nl} ")
        return
    else:
        pass
    return
def order(regims, birga_1, birga_2, val1_vol, val1, rate1, val2_vol, val2, val3_vol, val3, rate2, val4_vol, val4):
    while True:
            if birga_1 == 'alfa' and birga_2 == 'live':
                if val2 != 'USD' and val2 != 'USDT':
                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
                else:
                    reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                    reponse_b2 = live(val3, val4, rate2, val4_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
            elif birga_1 == 'live' and birga_2 == 'alfa':
                if val2 != 'USD' and val2 != 'USDT':
                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
                else:
                    reponse_b1 = live(val1, val2, rate1, val1_vol)
                    reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
            elif birga_1 == 'alfa' and birga_2 == 'hot':
                if val2 != 'USD' and val2 != 'USDT':
                    reponse_b1 = alfa(val1, val2, rate1, val2_vol)
                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
                else:
                    reponse_b1 = alfa(val1, val2, rate1, val1_vol)
                    reponse_b2 = hot(val3, val4, rate2, val4_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
            elif birga_1 == 'hot' and birga_2 == 'alfa':
                if val2 != 'USD' and val2 != 'USDT':
                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                    reponse_b2 = alfa(val3, val4, rate2, val3_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
                else:
                    reponse_b1 = hot(val1, val2, rate1, val1_vol)
                    reponse_b2 = alfa(val3, val4, rate2, val4_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
            elif birga_1 == 'hot' and birga_2 == 'live':
                if val2 != 'USD' or 'USDT':
                    reponse_b1 = hot(val1, val2, rate1, val2_vol)
                    reponse_b2 = live(val3, val4, rate2, val3_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
                else:
                    reponse_b1 = hot(val1, val2, rate1, val1_vol)
                    reponse_b2 = live(val3, val4, rate2, val4_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
            elif birga_1 == 'live' and birga_2 == 'hot':
                if val2 != 'USD' and val2 != 'USDT':
                    reponse_b1 = live(val1, val2, rate1, val2_vol)
                    reponse_b2 = hot(val3, val4, rate2, val3_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
                else:
                    reponse_b1 = live(val1, val2, rate1, val1_vol)
                    reponse_b2 = hot(val3, val4, rate2, val4_vol)
                    all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                            reponse_b1, reponse_b2, regims)
                    break
            else:
                all_csv(birga_1, birga_2, rate1, rate2, val1, val2, val4, val1_vol, val2_vol, val4_vol,
                        "No Such Command", "No Such Command", regims)
                break







