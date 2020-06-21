import os
import json
import time
import pandas as pd
import datetime as dt
import sqlite3

#################################   SHOW ALL ROWS & COLS   ####################################
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)
main_path_data = os.path.abspath("./data")
second_path_data = os.path.abspath(r"C:/Arbitrage/data")


def fast_refresh():
    t1 = time.time()

    conn = sqlite3.connect("kurses.db")
    new_kurses = pd.read_sql_query("SELECT * FROM kurses", conn)
    conn.close()

    new_regims_f = open(main_path_data + "\\new_regims.json", 'r')
    new_regims = json.load(new_regims_f)
    new_regims_f.close()

    bal = pd.read_csv(main_path_data + "\\balance.csv")
    balances2 = bal.to_json(orient='records')
    balances = json.loads(balances2)

    # print(balances)

    regim = []
    timed = []
    b1 = []
    b2 = []
    val1 = []
    val2 = []
    val3 = []
    kurs1 = []
    kurs2 = []
    minVol = []
    vol1 = []
    vol2 = []
    vol3 = []
    vol4 = []
    profit = []
    cash = []
    Go = []



    for k,v in new_regims.items():

        reg = k
        para1 = v['val2']+'/'+v['val1']
        para2 = v['val2']+'/'+v['val3']

        test_vol1 = float(new_kurses.loc[(new_kurses['birga'] == v['birga1']) & (new_kurses['direction'] == 'sell') & (
                    new_kurses['para_valut'] == para1), 'amount'].iloc[0])
        test_vol2 = float(new_kurses.loc[(new_kurses['birga'] == v['birga2']) & (new_kurses['direction'] == 'buy') & (
                    new_kurses['para_valut'] == para2), 'amount'].iloc[0])

        vol = [test_vol1, test_vol2]
        minvol2 = min(vol)
        minvol = float(minvol2) * float(v['per'])/100

        now = dt.datetime.now()
        timerr = now.strftime("%H:%M:%S")

        price1sell = float(new_kurses.loc[(new_kurses['birga'] == v['birga1']) & (new_kurses['direction'] == 'sell') & (
                    new_kurses['para_valut'] == para1), 'price'].iloc[0])
        # price2sell = float(new_kurses.loc[(new_kurses['birga'] == v['birga2']) & (new_kurses['direction'] == 'sell') & (
        #             new_kurses['para_valut'] == para1), 'price'].iloc[0])
        # price1buy = float(new_kurses.loc[(new_kurses['birga'] == v['birga1']) & (new_kurses['direction'] == 'buy') & (
        #             new_kurses['para_valut'] == para1), 'price'].iloc[0])
        price2buy = float(new_kurses.loc[(new_kurses['birga'] == v['birga2']) & (new_kurses['direction'] == 'buy') & (
                    new_kurses['para_valut'] == para2), 'price'].iloc[0])

        for k in balances:
            if k['Valuta'] == v['val1']:
                cas1 = k[v['birga1']]
            elif k['Valuta'] == v['val2']:
                cas2 = k[v['birga2']]
            else:
                pass

        minvol1 = (float(v['order']) * price1sell) + ((float(v['order']) * price1sell) * float(v['birga1_com']) / 100)
        minvol2 = float(v['order'])

        ##### check which balance is less  ########

        vv1 = cas1
        vv21 = (cas2 * price1sell) + ((cas2 * price1sell) * float(v['birga1_com']) / 100)

        if cas1 > vol11 and cas2 > vol22:
            cass = 'bank'
            if v['birga1'] == 'alfa':
                # vol11 = float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol
                vol11 = price1sell * minvol
                vol22 = minvol
                vol33 = minvol - (minvol * float(v['birga1_com']) / 100)
                vol44 = (vol33 * price2buy) - ((vol33 * price2buy) * float(v['birga2_com']) / 100)
                # vol44 = vol33 * float(new_kurses[v['birga2']][para2]['buy']['price'])
            else:
                # vol11 = (float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol) + ((float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol) * float(v['birga1_com'])/ 100)
                vol11 = price1sell * minvol + ((price1sell * minvol) * float(v['birga1_com']) / 100)
                vol22 = minvol
                vol33 = minvol
                vol44 = (vol33 * price2buy) - ((vol33 * price2buy) * float(v['birga2_com']) / 100)
                # vol44 = (vol33 * float(new_kurses[v['birga2']][para2]['buy']['price'])) - (vol33 * float(new_kurses[v['birga2']][para2]['buy']['price']) * float(v['birga2_com'])/ 100)
        elif cas1 < vol11 or cas2 < vol22:
            if cas1 > minvol1 and cas2 > minvol2:
                cass = 'balance'
                if vv1 < vv21:
                    if v['birga1'] == 'alfa':
                        # vol11 = float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol
                        vol11 = vv1
                        vol22 = (price1sell / vv1)
                        vol33 = vol22 - (vol22 * float(v['birga1_com']) / 100)
                        vol44 = (vol33 * price2buy) - ((vol33 * price2buy) * float(v['birga2_com']) / 100)
                        # vol44 = vol33 * float(new_kurses[v['birga2']][para2]['buy']['price'])
                    else:
                        # vol11 = (float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol) + ((float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol) * float(v['birga1_com'])/ 100)
                        vol11 = vv1
                        vol22 = (vv1 / price1sell) - ((vv1 / price1sell) * float(v['birga1_com']) / 100)
                        vol33 = vol22
                        vol44 = (vol33 * price2buy) - ((vol33 * price2buy) * float(v['birga2_com']) / 100)
                        # vol44 = (vol33 * float(new_kurses[v['birga2']][para2]['buy']['price'])) - (vol33 * float(new_kurses[v['birga2']][para2]['buy']['price']) * float(v['birga2_com'])/ 100)
                else:
                    if v['birga1'] == 'alfa':
                        # vol11 = float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol
                        vol11 = (vv21 + (vv21 * float(v['birga1_com']) / 100)) * price1sell
                        vol22 = vv21 + (vv21 * float(v['birga1_com']) / 100)
                        vol33 = vv21
                        vol44 = (vol33 * price2buy) - ((vol33 * price2buy) * float(v['birga2_com']) / 100)
                        # vol44 = vol33 * float(new_kurses[v['birga2']][para2]['buy']['price'])
                    else:
                        # vol11 = (float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol) + ((float(new_kurses[v['birga1']][para1]['sell']['price']) * minvol) * float(v['birga1_com'])/ 100)
                        vol11 = vv21 * price1sell + ((vv1 / price1sell) * float(v['birga1_com']) / 100)
                        vol22 = vv21
                        vol33 = vv21
                        vol44 = (vol33 * price2buy) - ((vol33 * price2buy) * float(v['birga2_com']) / 100)
                        # vol44 = (vol33 * float(new_kurses[v['birga2']][para2]['buy']['price'])) - (vol33 * float(new_kurses[v['birga2']][para2]['buy']['price']) * float(v['birga2_com'])/ 100)
            else:
                cass = 'poor'
                vol11 = 0
                vol22 = 0
                vol33 = 0
                vol44 = 0
        else:
            cass = 'poor'
            vol11 = 0
            vol22 = 0
            vol33 = 0
            vol44 = 0



        proff = (vol44 - vol11) / vol11 * 100


        if cass == 'balance' and proff > float(v['profit']):
            GG = v['avtomat']
        elif cass == 'bank' and proff >float(v['profit']):
            GG = v['avtomat']
        else:
            GG = v['avtomat']

        regim.append(reg)
        timed.append(timerr)
        b1.append(v['birga1'])
        b2.append(v['birga2'])
        val1.append(v['val1'])
        val2.append(v['val2'])
        val3.append(v['val3'])
        kurs1.append(price1sell)
        kurs2.append(price2buy)
        minVol.append(minvol)
        vol1.append(vol11)
        vol2.append(vol22)
        vol3.append(vol33)
        vol4.append(vol44)
        profit.append(proff)
        cash.append(cass)
        Go.append(GG)
    dw = {'regim': regim,
          'timed': timed,
          'b1': b1,
          'b2': b2,
          'val1': val1,
          'val2': val2,
          'val3': val3,
          'kurs1': kurs1,
          'kurs2': kurs2,
          'minVol': minVol,
          'vol1': vol1,
          'vol2': vol2,
          'vol3': vol3,
          'vol4': vol4,
          'profit': profit,
          'cash': cash,
          'Go': Go,
          }

    df = pd.DataFrame(data=dw)
    df.drop_duplicates(inplace=True)
    df.index = range(len(df))
    final_df = df.sort_values(by=['profit'], ascending=True)

    if final_df.shape[0]>0:
        final_df.to_csv(second_path_data + "\\vilki.csv", header=True, index=False)
    else:
        dw2 = {'regim': 'пусто',
              'timed': 'пусто',
              'b1': 'пусто',
              'b2': 'пусто',
              'val1': 'пусто',
              'val2': 'пусто',
              'val3': 'пусто',
              'kurs1': 'пусто',
              'kurs2': 'пусто',
              'minVol': 'пусто',
              'vol1': 'пусто',
              'vol2': 'пусто',
              'vol3': 'пусто',
              'vol4': 'пусто',
              'profit': 'пусто',
              'cash': 'пусто',
              'Go': 'пусто',
              }
        df2 = pd.DataFrame(data=dw2)
        df2.drop_duplicates(inplace=True)
        df2.to_csv(second_path_data + "\\vilki.csv", header=True, index=False)

    if final_df.shape[0] > 0:
        filter1 = final_df[(final_df['cash'] == 'balance') & (final_df['Go'] == 'on')]
        filter2 = final_df[(final_df['cash'] == 'bank') & (final_df['Go'] == 'on')]
        if filter1.shape[0] > 0:
            # NewOrders.order(filter1.iloc[0]['regim'],
            #                 filter1.iloc[0]['b1'],
            #                 filter1.iloc[0]['b2'],
            #                 filter1.iloc[0]['vol1'],
            #                 filter1.iloc[0]['val1'],
            #                 filter1.iloc[0]['kurs1'],
            #                 filter1.iloc[0]['vol2'],
            #                 filter1.iloc[0]['val2'],
            #                 filter1.iloc[0]['vol3'],
            #                 filter1.iloc[0]['val2'],
            #                 filter1.iloc[0]['kurs2'],
            #                 filter1.iloc[0]['vol4'],
            #                 filter1.iloc[0]['val3'])
            # Balance.NewBalance()
            pass
        elif filter2.shape[0] > 0:
            # NewOrders.order(filter2.iloc[0]['regim'],
            #                 filter2.iloc[0]['b1'],
            #                 filter2.iloc[0]['b2'],
            #                 filter2.iloc[0]['vol1'],
            #                 filter2.iloc[0]['val1'],
            #                 filter2.iloc[0]['kurs1'],
            #                 filter2.iloc[0]['vol2'],
            #                 filter2.iloc[0]['val2'],
            #                 filter2.iloc[0]['vol3'],
            #                 filter2.iloc[0]['val2'],
            #                 filter2.iloc[0]['kurs2'],
            #                 filter2.iloc[0]['vol4'],
            #                 filter2.iloc[0]['val3'])
            # Balance.NewBalance()
            pass
        else:
            pass
    else:
        pass

    print(final_df)


    t2 = time.time()
    # print("ALL TIME :", t2-t1)

if __name__ == "__main__":
    while True:
        try:
            fast_refresh()
            time.sleep(0.3)
        except Exception as e:
            print(e)
            # var = {"alfa": {"PZM/USD": {"sell": {"price": "0.1", "amount": "0.1"}, "buy": {"price": "0.1", "amount": "0.1"}},
            #           "PZM/BTC": {"sell": {"price": "0.1", "amount": "0.1"}, "buy": {"price": "0.1", "amount": "0.1"}}},
            #  "hot": {"PZM/USDT": {"sell": {"price": "0.1", "amount": "0.1"}, "buy": {"price": "0.1", "amount": "0.1"}},
            #          "PZM/BTC": {"sell": {"price": "0.1", "amount": "0.1"}, "buy": {"price": "0.1", "amount": "0.1"}}},
            #  "live": {"PZM/USD": {"sell": {"price": "0.1", "amount": "0.1"}, "buy": {"price": "0.1", "amount": "0.1"}},
            #           "PZM/BTC": {"sell": {"price": "0.1", "amount": "0.1"}, "buy": {"price": "0.1", "amount": "0.1"}}}}
            # os.remove(main_path_data + "\\kurses.json")
            # with open(main_path_data + "\\kurses.json", "w") as p:
            #     json.dump(var, p)
            time.sleep(5)
