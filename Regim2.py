import os
import json
import pandas as pd
import datetime as dt
import time
import requests
import Reg2_Orders
import Finished_orders
import Balance


#################################   SHOW ALL ROWS & COLS   ####################################
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)

main_path_data = os.path.abspath("./data")
second_path_data = os.path.abspath(r"C:/inetpub/wwwroot/WBW/data")

a_file1 = open(second_path_data + "\\rools.json", "r")
rools = json.load(a_file1)
a_file1.close()

a_file2 = open(second_path_data + "\\rools2.json", "r")
rools2 = json.load(a_file2)
a_file2.close()

def bot_sendtext(bot_message):
    ##########################    Telegram    ################################

    ad = open(second_path_data + "\\keys.json", "r")
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

def regim():

    all_vilki2 = pd.read_csv(second_path_data + "\\vilki2_all.csv")
    fids = pd.DataFrame()
    for k,v in regims2.items():
        if v['avtomat'] == 'on':
            dft = vilki_all[
                (vilki_all["birga_x"] == v["birga1"]) &
                      (vilki_all["birga_y"] == v["birga2"]) &
                      (vilki_all["valin_x"] == v["val1"]) &
                      (vilki_all["valout_x"] == v["val2"]) &
                      (vilki_all["Vol3"] > 0) &
                      (vilki_all["volume"] > float(v["order"])) &
                      (vilki_all["valout_y"] == v["val3"])
            ]

            if dft.shape[0]>0:
                dft.loc[:, 'rates_x'] = dft['rates_x'].map('{:,.9f}'.format)
                dft.loc[:, 'rates_y'] = dft['rates_y'].map('{:,.9f}'.format)
                bir = v["birga1"]
                dft = dft[dft['percent'] == dft['percent'].max()]

                dft.loc[:, 'regim'] = k
                dft.loc[:, 'min_A'] = v["order"]

                if v["val1"] == "BTC":
                    if bir == 'hot':
                        dft.loc[:, 'new_kurs'] = hs_PU2.iloc[0]['rates']
                        dft.loc[:, 'min_kurs'] = hot2.iloc[0]['rates']
                    elif bir == 'live':
                        dft.loc[:, 'new_kurs'] = ls_PU2.iloc[0]['rates']
                        dft.loc[:, 'min_kurs'] = live2.iloc[0]['rates']
                    elif bir == 'alfa':
                        dft.loc[:, 'new_kurs'] = as_PU2.iloc[0]['rates']
                        dft.loc[:, 'min_kurs'] = alfa2.iloc[0]['rates']
                else:
                    if bir == 'hot':
                        dft.loc[:, 'new_kurs'] = hs_PU.iloc[0]['rates']
                        dft.loc[:, 'min_kurs'] = hot.iloc[0]['rates']
                    elif bir == 'live':
                        dft.loc[:, 'new_kurs'] = ls_PU.iloc[0]['rates']
                        dft.loc[:, 'min_kurs'] = live.iloc[0]['rates']
                    elif bir == 'alfa':
                        dft.loc[:, 'new_kurs'] = as_PU.iloc[0]['rates']
                        dft.loc[:, 'min_kurs'] = alfa.iloc[0]['rates']

                tickers_all = ['BTC/USD', 'PZM/USD', 'PZM/USDT', 'ETH/USD', 'ETH/USDT', 'PZM/BTC', 'ETH/BTC']

                parametr1 = "{}/{}".format(v["val1"], v["val2"])
                parametr2 = "{}/{}".format(v["val2"], v["val1"])

                for i in tickers_all:
                    if i == parametr1:
                        para = i
                    elif i == parametr2:
                        para = i

                dft.loc[:, 'My_kurs'] = float(dft.iloc[0]['new_kurs']) + (float(rools2[bir]['amount_precision'][para]) * int(v['per']))
                dft.loc[:, '0_kurs'] = float(dft.iloc[0]['rates_y']) - (float(dft.iloc[0]['rates_y']) * float(v["profit"])/100)

                dft['Vol3'] = dft['Vol3'].apply(pd.to_numeric, errors='coerce')
                dft['Vol4'] = dft['Vol4'].apply(pd.to_numeric, errors='coerce')
                dft['My_kurs'] = dft['My_kurs'].apply(pd.to_numeric, errors='coerce')


                if bir == 'alfa':
                    dft.loc[:, 'Vol2'] = dft.iloc[0]['Vol3']+(dft.iloc[0]['Vol3'] * dft.iloc[0]['Com_x'] / 100)
                    dft.loc[:, 'Vol1'] = dft.iloc[0]['Vol2'] * float(dft.iloc[0]['My_kurs'])
                else:
                    dft.loc[:, 'Vol2'] = dft.iloc[0]['Vol3']
                    dft.loc[:, 'Vol1'] = dft.iloc[0]['Vol3'] * dft.iloc[0]['My_kurs'] + (dft.iloc[0]['Vol3'] * dft.iloc[0]['My_kurs'] * dft.iloc[0]['Com_x'] / 100)

                dft.loc[:, 'prof'] = dft['Vol4'] - dft['Vol1']

                dft.loc[:, 'per'] = dft['prof'] / dft['Vol1'] * 100
                dft.loc[:, 'New_per'] = float(v["profit"])
                dft.drop(['index'], axis=1, inplace=True)

                df_vilki = vilki2[vilki2["regim"] == int(k)]

                if df_vilki.shape[0] > 0:
                    my_order = int(df_vilki.iloc[0]['order_id'])
                    my_dict = finished_orders[df_vilki.iloc[0]['birga_x']]
                    if str(my_order) in my_dict:
                        if my_dict[str(my_order)] >= float(df_vilki.iloc[0]['min_A']):
                            if float(my_dict[str(my_order)]) >= float(df_vilki.iloc[0]['Vol2']):
                                birga = df_vilki.iloc[0]['birga_y']
                                val3 = df_vilki.iloc[0]['valin_y']
                                val4 = df_vilki.iloc[0]['valout_y']
                                rate2 = str(df_vilki.iloc[0]['rates_y'])
                                val3_vol = df_vilki.iloc[0]['Vol3']

                                if birga == 'alfa':
                                    reponse = Reg2_Orders.alfa(val3, val4, rate2, val3_vol)
                                elif birga == 'live':
                                    reponse = Reg2_Orders.live(val3, val4, rate2, val3_vol)
                                elif birga == 'hot':
                                    reponse = Reg2_Orders.hot(val3, val4, rate2, val3_vol)
                                else:
                                    reponse = "No such BIRGA"

                                now = dt.datetime.now()
                                timer2 = now.strftime("%Y-%m-%d %H:%M:%S")
                                profit = (df_vilki.iloc[0]['Vol4'] - df_vilki.iloc[0]['Vol1']) / df_vilki.iloc[0]['Vol1'] * 100

                                dw2 = {'regim': df_vilki.iloc[0]['regim'],
                                       'timed': [timer2],
                                       'b1': [df_vilki.iloc[0]['birga_x']],
                                       'b2': [df_vilki.iloc[0]['birga_y']],
                                       'val1': [df_vilki.iloc[0]['valin_x']],
                                       'val2': [df_vilki.iloc[0]['valout_x']],
                                       'val3': [df_vilki.iloc[0]['valout_y']],
                                       'kurs1': [df_vilki.iloc[0]['rates_x']],
                                       'kurs2': [df_vilki.iloc[0]['rates_y']],
                                       'Vol1': [df_vilki.iloc[0]['Vol1']],
                                       'Vol2': [df_vilki.iloc[0]['Vol2']],
                                       'Vol3': [df_vilki.iloc[0]['Vol3']],
                                       'Vol4': [df_vilki.iloc[0]['Vol4']],
                                       'profit': [profit],
                                       'rep1': [reponse],
                                       'rep2': [my_order],
                                       }
                                df2 = pd.DataFrame(data=dw2, index=[0])
                                df2.drop_duplicates(inplace=True)
                                all_vilki2 = pd.concat([dft, all_vilki2], ignore_index=True, join='outer')
                                nl = '\n'
                                bot_sendtext(
                                    f" ЕСТЬ ВИЛКА: {nl} РЕЖИМ : {df_vilki.iloc[0]['regim']} {nl} {df_vilki.iloc[0]['birga_x']} / {df_vilki.iloc[0]['birga_y']} {nl} "
                                    f"{reponse} / {my_order} {nl} {df_vilki.iloc[0]['valin_x']} -> {df_vilki.iloc[0]['valout_x']} -> {df_vilki.iloc[0]['valout_y']} {nl} "
                                    f"PROFIT: {profit} {nl} ")

                                vilki2.drop(vilki2.index[vilki2["regim"] == int(k)], inplace=True)

                                ###############  delete from finished json  #############################

                                a_file = open(main_path_data + "\\finished_orders.json", "r")
                                json_object = json.load(a_file)
                                a_file.close()

                                del json_object[df_vilki.iloc[0]['birga_x']][str(my_order)]

                                a_file = open(main_path_data + "\\finished_orders.json", "w")
                                json.dump(json_object, a_file)
                                a_file.close()
                                Balance.NewBalance()
                            else:
                                birga = df_vilki.iloc[0]['birga_x']
                                val3 = df_vilki.iloc[0]['valin_y']
                                val4 = df_vilki.iloc[0]['valout_y']
                                rate2 = df_vilki.iloc[0]['rates_y']
                                birga2 = df_vilki.iloc[0]['birga_y']
                                nl = '\n'
                                if birga == 'alfa':
                                    val1_vol = (my_dict[str(my_order)] * df_vilki.iloc[0]['My_kurs'])
                                    val2_vol = my_dict[str(my_order)]
                                    val3_vol = my_dict[str(my_order)] - (my_dict[str(my_order)] * df_vilki.iloc[0]['Com_x'] / 100)
                                    val4_vol = (val3_vol * df_vilki.iloc[0]['rates_y']) - (
                                                val3_vol * df_vilki.iloc[0]['rates_y'] * df_vilki.iloc[0]['Com_y'] / 100)
                                else:
                                    val1_vol = (my_dict[str(my_order)] * df_vilki.iloc[0]['My_kurs']) + (
                                                my_dict[str(my_order)] * df_vilki.iloc[0]['rates_x'] * df_vilki.iloc[0][
                                            'Com_x'] / 100)
                                    val2_vol = my_dict[str(my_order)]
                                    val3_vol = my_dict[str(my_order)]
                                    val4_vol = (val3_vol * df_vilki.iloc[0]['rates_y']) - (
                                                val3_vol * df_vilki.iloc[0]['rates_y'] * df_vilki.iloc[0]['Com_y'] / 100)

                                if birga2 == 'alfa':
                                    reponse = Reg2_Orders.alfa(val3, val4, str(rate2), val3_vol)
                                elif birga2 == 'live':
                                    reponse = Reg2_Orders.live(val3, val4, str(rate2), val3_vol)
                                elif birga2 == 'hot':
                                    reponse = Reg2_Orders.hot(val3, val4, str(rate2), val3_vol)
                                else:
                                    reponse = "No such BIRGA"

                                if birga == 'alfa':
                                    reponse2 = Reg2_Orders.alfa_cancel(str(my_order))
                                elif birga == 'live':
                                    reponse2 = Reg2_Orders.live_cancel(df_vilki.iloc[0]['valin_x'],
                                                                       df_vilki.iloc[0]['valout_x'], str(my_order))
                                elif birga == 'hot':
                                    reponse2 = Reg2_Orders.hot_cancel(df_vilki.iloc[0]['valin_x'],
                                                                      df_vilki.iloc[0]['valout_x'], str(my_order))
                                else:
                                    reponse2 = "No such BIRGA"
                                now = dt.datetime.now()
                                timer2 = now.strftime("%Y-%m-%d %H:%M:%S")

                                profit = (val4_vol - val1_vol) / val1_vol * 100
                                dw2 = {'regim': df_vilki.iloc[0]['regim'],
                                       'timed': [timer2],
                                       'b1': [df_vilki.iloc[0]['birga_x']],
                                       'b2': [df_vilki.iloc[0]['birga_y']],
                                       'val1': [df_vilki.iloc[0]['valin_x']],
                                       'val2': [df_vilki.iloc[0]['valout_x']],
                                       'val3': [df_vilki.iloc[0]['valout_y']],
                                       'kurs1': [df_vilki.iloc[0]['My_kurs']],
                                       'kurs2': [df_vilki.iloc[0]['rates_y']],
                                       'Vol1': [val1_vol],
                                       'Vol2': [val2_vol],
                                       'Vol3': [val3_vol],
                                       'Vol4': [val4_vol],
                                       'profit': [profit],
                                       'rep1': [reponse],
                                       'rep2': [my_order],
                                       }
                                df2 = pd.DataFrame(data=dw2, index=[0])
                                df2.drop_duplicates(inplace=True)
                                all_vilki2 = pd.concat([dft, all_vilki2], ignore_index=True, join='outer')

                                bot_sendtext(
                                    f" ЕСТЬ ВИЛКА: {nl} РЕЖИМ : {df_vilki.iloc[0]['regim']} {nl} {df_vilki.iloc[0]['birga_x']} / {df_vilki.iloc[0]['birga_y']} {nl} "
                                    f"{reponse} / {my_order} {nl} {df_vilki.iloc[0]['valin_x']} -> {df_vilki.iloc[0]['valout_x']} -> {df_vilki.iloc[0]['valout_y']} {nl} "
                                    f"PROFIT: {profit} {nl} ")

                                vilki2.drop(vilki2.index[vilki2["regim"] == int(k)], inplace=True)

                                ###############  delete from finished json  #############################

                                a_file32 = open(main_path_data + "\\finished_orders.json", "r")
                                json_object = json.load(a_file32)
                                a_file32.close()

                                del json_object[df_vilki.iloc[0]['birga_x']][str(my_order)]

                                a_file23 = open(main_path_data + "\\finished_orders.json", "w")
                                json.dump(json_object, a_file23)
                                a_file23.close()
                                Balance.NewBalance()
                        else:
                            del my_dict[str(my_order)]
                            f33 = open(main_path_data + "\\finished_orders.json", "w")
                            json.dump(my_dict, f33)
                            f33.close()
                    else:
                        print("######  dft  ######", '\n', dft, '\n')
                        adf = dft[(dft['per'] >= dft['New_per']) &
                                  # (float(dft['My_kurs']) <= float(df_vilki.iloc[0]['My_kurs'])) &
                                  (float(dft['rates_y']) >= float(df_vilki.iloc[0]['rates_y'])) &
                                  (dft['Vol2'] >= float(df_vilki.iloc[0]['Vol2']))]

                        # print( "PERC :", dft.iloc[0]['per'], "___###___", "PERC NEW :",dft.iloc[0]['New_per'])
                        # print("My_kurs :", dft.iloc[0]['My_kurs'], "___###___", "My_kurs OLD :", df_vilki.iloc[0]['My_kurs'])
                        # print("rates_y :", dft.iloc[0]['rates_y'], "___###___", "rates_y OLD :", df_vilki.iloc[0]['rates_y'])
                        # print( "Vol2 :", dft.iloc[0]['Vol2'], "___###___", "Vol2 OLD :",df_vilki.iloc[0]['Vol2'])


                        print('\n',"######  adf  ######", '\n', adf, '\n')
                        if adf.shape[0]>0:
                            pass
                        else:
                            bir = df_vilki.iloc[0]['birga_x']
                            if bir == 'alfa':
                                reponse2 = Reg2_Orders.alfa_cancel(df_vilki.iloc[0]['order_id'])
                            elif bir == 'live':
                                reponse2 = Reg2_Orders.live_cancel(df_vilki.iloc[0]['valin_x'],df_vilki.iloc[0]['valout_x'],df_vilki.iloc[0]['order_id'])
                            elif bir == 'hot':
                                reponse2 = Reg2_Orders.hot_cancel(df_vilki.iloc[0]['valin_x'],df_vilki.iloc[0]['valout_x'],df_vilki.iloc[0]['order_id'])
                            else:
                                reponse2 = "No such BIRGA"
                            vilki2.drop(vilki2.index[vilki2["regim"] == int(k)], inplace=True)

                            ###############  delete from finished json  #############################

                            a_file = open(main_path_data + "\\finished_orders.json", "r")
                            json_object = json.load(a_file)
                            a_file.close()
                            # print("######",str(int(df_vilki.iloc[0]['order_id'])))

                            if str(int(df_vilki.iloc[0]['order_id'])) in json_object[bir]:
                                del json_object[bir][str(int(df_vilki.iloc[0]['order_id']))]

                            a_file = open(main_path_data + "\\finished_orders.json", "w")
                            json.dump(json_object, a_file)
                            a_file.close()

                else:
                    # print("######  dft  ######", '\n', dft, '\n')
                    dft = dft[(dft["per"] > float(dft.iloc[0]["New_per"])) &
                    (dft["Vol2"] > float(dft.iloc[0]["min_A"])) &
                     (dft['My_kurs'] < float(dft.iloc[0]["min_kurs"]))
                    ]

                    # print(dft)
                    if dft.shape[0]>0:
                        val1 = dft.iloc[0]['valin_x']
                        val2 = dft.iloc[0]['valout_x']
                        rate1 = str(dft.iloc[0]['My_kurs'])
                        val2_vol = dft.iloc[0]['Vol2']

                        if bir == 'alfa':
                            reponse = Reg2_Orders.alfa(val1, val2, rate1, val2_vol)
                        elif bir == 'live':
                            reponse = Reg2_Orders.live(val1, val2, rate1, val2_vol)
                        elif bir == 'hot':
                            reponse = Reg2_Orders.hot(val1, val2, rate1, val2_vol)
                        else:
                            reponse = "No such BIRGA"
                            pass
                        dft.loc[:,'order_id'] = reponse
                        vilki2 = pd.concat([dft, vilki2], ignore_index=True, join='outer')

                fids = pd.concat([dft, fids], ignore_index=True, join='outer')

    vilki2.to_csv(second_path_data + "\\vilki2.csv", header=True, index=False)
    all_vilki2.to_csv(second_path_data + "\\vilki2_all.csv", header=True, index=False)

if __name__ == "__main__":
    while True:
        try:
            t1 = time.time()

            with open(main_path_data + "\\live_bd_PU.csv", 'r') as f:
                live_bd = pd.read_csv(f)
                f.close()
            with open(main_path_data + "\\alfa_bd_PU.csv", 'r') as f:
                alfa_bd = pd.read_csv(f)
                f.close()
            with open(main_path_data + "\\hot_bd_PU.csv", 'r') as f:
                hot_bd = pd.read_csv(f)
                f.close()
            with open(main_path_data + "\\live_bd_PB.csv", 'r') as f:
                live_bd2 = pd.read_csv(f)
                f.close()
            with open(main_path_data + "\\alfa_bd_PB.csv", 'r') as f:
                alfa_bd2 = pd.read_csv(f)
                f.close()
            with open(main_path_data + "\\hot_bd_PB.csv", 'r') as f:
                hot_bd2 = pd.read_csv(f)
                f.close()
            with open(main_path_data + "\\vilki_all.csv", 'r') as f:
                vilki_all = pd.read_csv(f)
                f.close()

            regims_f = open(second_path_data + "\\regims2.json", 'r')  # second_path
            regims2 = json.load(regims_f)
            regims_f.close()

            hot = hot_bd[hot_bd['direction'] == 'buy'].head(1)
            hs_PU = hot_bd[hot_bd['direction'] == 'sell'].head(1)

            alfa = alfa_bd[alfa_bd['direction'] == 'buy'].head(1)
            as_PU = alfa_bd[alfa_bd['direction'] == 'sell'].head(1)

            live = live_bd[live_bd['direction'] == 'buy'].head(1)
            ls_PU = live_bd[live_bd['direction'] == 'sell'].head(1)

            hot2 = hot_bd2[hot_bd2['direction'] == 'buy'].head(1)
            hs_PU2 = hot_bd2[hot_bd2['direction'] == 'sell'].head(1)

            alfa2 = alfa_bd2[alfa_bd2['direction'] == 'buy'].head(1)
            as_PU2 = alfa_bd2[alfa_bd2['direction'] == 'sell'].head(1)

            live2 = live_bd2[live_bd2['direction'] == 'buy'].head(1)
            ls_PU2 = live_bd2[live_bd2['direction'] == 'sell'].head(1)

            fo = open(main_path_data + "\\finished_orders.json", 'r')
            finished_orders = json.load(fo)
            fo.close()

            vilki2 = pd.read_csv(second_path_data + "\\vilki2.csv")
            vilki2 = Finished_orders.main(vilki2,hot,alfa,live,hot2,alfa2,live2)
            # t2 = time.time()
            regim()
            t3 = time.time()
            # print("open orders :", t2 - t1)
            # print("regim 2 :", t3 - t2)
            print("ALL TIME :", t3 - t1)
            time.sleep(0.4)

        except Exception as e:
            print(e)
            time.sleep(0.2)