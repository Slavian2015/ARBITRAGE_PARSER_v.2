import os
import json
import pandas as pd
import datetime as dt
import time
import NewOrders
import Balance

#################################   SHOW ALL ROWS & COLS   ####################################
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)
main_path_data = os.path.abspath("./data")
second_path_data = os.path.abspath(r"C:/inetpub/wwwroot/WBW/data")

def fast_refresh():

    df = pd.DataFrame()
    dddd = pd.DataFrame()
    dfs = pd.DataFrame()
    final = pd.DataFrame()
    dfs2 = pd.DataFrame()
    result = pd.DataFrame()
    filter = pd.DataFrame()
    alfa_bd = pd.DataFrame()
    alfa_bd2 = pd.DataFrame()
    hot_bd2 = pd.DataFrame()
    live_bd2 = pd.DataFrame()
    live_bd = pd.DataFrame()
    hot_bd = pd.DataFrame()


    alfa_bd = pd.read_csv(main_path_data + "\\alfa_bd_PU.csv", thousands=',')
    alfa_bd2 = pd.read_csv(main_path_data + "\\alfa_bd_PB.csv", thousands=',')

    hot_bd2 = pd.read_csv(main_path_data + "\\hot_bd_PB.csv", thousands=',')
    live_bd2 = pd.read_csv(main_path_data + "\\live_bd_PB.csv", thousands=',')
    try:
        with open(main_path_data + "\\live_bd_PU.csv", 'r') as f:
            live_bd = pd.read_csv(f, thousands=',')
            f.close()
    except Exception as e:
        with open(main_path_data + "\\live_bd_PU.csv", 'r') as f:
            live_bd = pd.read_csv(f, thousands=',')
            f.close()

    try:
        with open(main_path_data + "\\hot_bd_PU.csv", 'r') as f:
            hot_bd = pd.read_csv(f, thousands=',')
            f.close()
    except Exception as e:
        print("EXCEPT to open HOT")
        time.sleep(0.1)
        with open(main_path_data + "\\hot_bd_PU.csv", 'r') as f:
            hot_bd = pd.read_csv(f, thousands=',')
            f.close()
            pass


    new_regims_f = open(second_path_data + "\\new_regims.json", 'r')
    new_regims = json.load(new_regims_f)
    new_regims_f.close()

    bal = pd.read_csv(second_path_data + "\\balance.csv")
    balances2 = bal.to_json(orient='records')
    balances = json.loads(balances2)

    new_com = open(second_path_data + "\\commis.json", 'r')
    com = json.load(new_com)
    new_com.close()

    frames = [alfa_bd, hot_bd, live_bd, alfa_bd2,
              hot_bd2, live_bd2
              ]
    df = pd.concat(frames, ignore_index=True)

    filter_a = df[df['birga'] == 'alfa'].index
    filter_l = df[df['birga'] == 'live'].index
    filter_h = df[df['birga'] == 'hot'].index

    a = com['main']["alfa"]
    l = com['main']["live"]
    h = com['main']["hot"]

    df.loc[(filter_a), 'Com'] = float(a)
    df.loc[(filter_h), 'Com'] = float(h)
    df.loc[(filter_l), 'Com'] = float(l)


    dfs2 = pd.merge(df, df, left_on=df['valout'], right_on=df['valin'], how='outer')
    dfs2['rates_x'] = dfs2['rates_x'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    dfs2['rates_y'] = dfs2['rates_y'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    dfs2['volume_x'] = dfs2['volume_x'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    dfs2['volume_y'] = dfs2['volume_y'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    dfs2.drop(['key_0'], axis = 1, inplace = True)
    dfs2['rates_y'] = dfs2['rates_y'].map('{:,.10f}'.format)
    dfs2['rates_x'] = dfs2['rates_x'].map('{:,.10f}'.format)


    ###############       Main dataframe with all data      ####################
    result = dfs2[(dfs2['valin_x'] == dfs2['valout_y'])]
    usdt = dfs2[(dfs2['valin_x'] == 'USD') & (dfs2['valout_y'] == 'USDT')]
    usd = dfs2[(dfs2['valin_x'] == 'USDT') & (dfs2['valout_y'] == 'USD')]
    final = result.append([usdt, usd])
    final.reset_index(inplace=True, drop = True)
    final.reset_index(level=0, inplace=True)
    filter = final[(final['birga_x'] == final['birga_y']) &
                   (final['rates_x'] < final['rates_y']) &
                   (final['valin_x'] == final['valout_y'])
                   ]

    final = final.drop(filter['index'], axis=0)

    final['rates_x'] = final['rates_x'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['rates_y'] = final['rates_y'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['volume_x'] = final['volume_x'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['volume_y'] = final['volume_y'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['Com_x'] = final['Com_x'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['Com_y'] = final['Com_y'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')


    for i in balances:
        balanc_a = final[(final['birga_x'] == 'alfa') & (final['valin_x'] == i['Valuta'])].index
        balanc_h = final[(final['birga_x'] == 'hot') & (final['valin_x'] == i['Valuta'])].index
        balanc_l = final[(final['birga_x'] == 'live') & (final['valin_x'] == i['Valuta'])].index

        a_b = i['alfa']
        h_b = i['hot']
        l_b = i['live']

        final.loc[(balanc_a), 'Bal1'] = a_b / final.loc[(balanc_a), 'rates_x']
        final.loc[(balanc_h), 'Bal1'] = h_b / final.loc[(balanc_h), 'rates_x'] - (h_b/ final.loc[(balanc_h), 'rates_x'] * final.loc[(balanc_h), 'Com_x'] / 100)
        final.loc[(balanc_l), 'Bal1'] = l_b / final.loc[(balanc_l), 'rates_x'] - (l_b / final.loc[(balanc_l), 'rates_x'] * final.loc[(balanc_l), 'Com_x'] / 100)


        balanc_a2 = final[(final['birga_y'] == 'alfa') & (final['valin_y'] == i['Valuta'])].index
        balanc_h2 = final[(final['birga_y'] == 'hot') & (final['valin_y'] == i['Valuta'])].index
        balanc_l2 = final[(final['birga_y'] == 'live') & (final['valin_y'] == i['Valuta'])].index


        final.loc[(balanc_a2), 'Bal2'] = a_b
        final.loc[(balanc_h2), 'Bal2'] = h_b
        final.loc[(balanc_l2), 'Bal2'] = l_b

    reg2 = final
    final.loc[:,"volume"] = final[['volume_x','volume_y','Bal2','Bal1']].min(axis=1)

    final['volume'] = final['volume'].map('{:,.10f}'.format)
    final['volume'] = final['volume'].replace(',','', regex=True).apply(pd.to_numeric,errors='coerce')

    final.loc[:,'Vol1'] = final['rates_x'] * final['volume'] + (final['rates_x'] * final['volume'] * final['Com_x'] / 100)
    final.loc[:,'Vol2'] = final['volume']
    final.loc[:,'Vol3'] = final['volume']
    final.loc[:,'Vol4'] = final['rates_y'] * final['volume'] - (final['rates_y'] * final['volume'] * final['Com_y'] / 100)

    fil_a = final[final['birga_x'] == 'alfa'].index
    final.loc[(fil_a), 'Vol3'] = final.loc[(fil_a), 'Vol2'] - (final.loc[(fil_a), 'Vol2'] * final.loc[(fil_a), 'Com_x'] / 100)
    final.loc[(fil_a), 'Vol1'] = final.loc[(fil_a), 'rates_x'] * final.loc[(fil_a), 'volume']
    final.loc[(fil_a), 'Vol4'] = final.loc[(fil_a), 'rates_y'] * final.loc[(fil_a),'Vol3'] - (final.loc[(fil_a), 'rates_y'] * final.loc[(fil_a),'Vol3'] * final['Com_y'] / 100)

    final['Vol1'] = final['Vol1'].map('{:,.10f}'.format)
    final['Vol2'] = final['Vol2'].map('{:,.10f}'.format)
    final['Vol3'] = final['Vol3'].map('{:,.10f}'.format)
    final['Vol4'] = final['Vol4'].map('{:,.10f}'.format)

    final['Vol1'] = final['Vol1'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['Vol2'] = final['Vol2'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['Vol3'] = final['Vol3'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['Vol4'] = final['Vol4'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')

    final.loc[:,'profit'] = final['Vol4'] - final['Vol1']
    final.loc[:,'percent'] = final['profit'] / final['Vol1'] * 100

    final['profit'] = final['profit'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    final['percent'] = final['percent'].replace(',','', regex=True).apply(pd.to_numeric, errors='coerce')
    dddd = final.dropna()
    final = dddd
    final = final[final['direction_y'] != 'buy']
    
    #################################################################################################
    
    reg2.loc[:,"volume"] = reg2[['volume_y','Bal2','Bal1']].min(axis=1)


    reg2.loc[:,'Vol1'] = reg2['rates_x'] * reg2['volume'] + (reg2['rates_x'] * reg2['volume'] * reg2['Com_x'] / 100)
    reg2.loc[:,'Vol2'] = reg2['volume']
    reg2.loc[:,'Vol3'] = reg2['volume']
    reg2.loc[:,'Vol4'] = reg2['rates_y'] * reg2['volume'] - (reg2['rates_y'] * reg2['volume'] * reg2['Com_y'] / 100)


    fil_a = reg2[reg2['birga_x'] == 'alfa'].index
    reg2.loc[(fil_a), 'Vol3'] = reg2.loc[(fil_a), 'Vol2'] - (reg2.loc[(fil_a), 'Vol2'] * reg2.loc[(fil_a), 'Com_x'] / 100)
    reg2.loc[(fil_a), 'Vol1'] = reg2.loc[(fil_a), 'rates_x'] * reg2.loc[(fil_a), 'volume']
    reg2.loc[(fil_a), 'Vol4'] = reg2.loc[(fil_a), 'rates_y'] * reg2.loc[(fil_a),'Vol3'] - (reg2.loc[(fil_a), 'rates_y'] * reg2.loc[(fil_a),'Vol3'] * reg2['Com_y'] / 100)

    reg2.loc[:,'profit'] = reg2['Vol4'] - reg2['Vol1']
    reg2.loc[:,'percent'] = reg2['profit'] / reg2['Vol1'] * 100

    reg2['profit'] = reg2['profit'].apply(pd.to_numeric, errors='coerce')
    reg2['percent'] = reg2['percent'].apply(pd.to_numeric, errors='coerce')

    # reg2 = reg2[reg2['Vol2'].notnull()]
    # dddd33 = reg2.dropna()
    #
    # reg2 = dddd33
    reg2 = reg2[reg2['direction_y'] != 'buy']
    reg2.to_csv(main_path_data + "\\vilki_all.csv", header=True, index=False)
    #################################################################################################

    def regim_filter():
        fids = pd.DataFrame()
        for k,v in new_regims.items():
            dft = final[(final["birga_x"] == v["birga1"]) &
                      (final["birga_y"] == v["birga2"]) &
                      (final["valin_x"] == v["val1"]) &
                      (final["valout_x"] == v["val2"]) &
                      (final["valout_y"] == v["val3"]) &
                      (final["percent"] > float(v["profit"])) &
                      (final["Vol2"] > float(v["order"]))
            ]


            per = (float(v["per"]) / 100)
            dft.loc[:,'Vol1'] = dft['Vol1'] * per
            dft.loc[:,'Vol2'] = dft['Vol2'] * per
            dft.loc[:,'Vol3'] = dft['Vol3'] * per
            dft.loc[:,'Vol4'] = dft['Vol4'] * per

            dft = dft[(dft["Vol3"] > float(v["order"]))]


            now = dt.datetime.now()
            timerr = now.strftime("%H:%M:%S")

            dft['avtomat'] = v["avtomat"]
            dft['regim'] = k
            dft['timed'] = timerr

            dft = dft.sort_values(by=['Vol2'], ascending=False)
            dft = dft.head(1)

            fids = pd.concat([dft, fids], ignore_index=True, join='outer')
        return fids


    dfs = regim_filter()
    dfs = dfs.reindex(columns=['regim',
                               'timed',
                               'birga_x',
                               'birga_y',
                               'valin_x',
                               'valout_x',
                               'valout_y',
                               'rates_x',
                               'rates_y',
                               'Vol1',
                               'Vol2',
                               'Vol3',
                               'Vol4',
                               'percent',
                               'avtomat'])
    dfs.columns = ['regim',
                   'timed',
                   'b1',
                   'b2',
                   'val1',
                   'val2',
                   'val3',
                   'kurs1',
                   'kurs2',
                   'Vol1',
                   'Vol2',
                   'Vol3',
                   'Vol4',
                   'profit',
                   'Go']

    dfs['profit'] = dfs['profit'].map('{:,.2f}'.format)



    dfs.index = range(len(dfs))
    dfs = dfs.sort_values(by=['profit'], ascending=False)

    now = dt.datetime.now()
    print('\n', now.strftime("%H:%M:%S"), '\n')

    if dfs.shape[0] > 0:
        dfs.to_csv(second_path_data + "\\vilki.csv", header=True, index=False)
        filter1 = dfs[(dfs['Go'] == 'on')]
        if filter1.shape[0] > 0:
            NewOrders.order(filter1.iloc[0]['regim'],
                            filter1.iloc[0]['b1'],
                            filter1.iloc[0]['b2'],
                            filter1.iloc[0]['Vol1'],
                            filter1.iloc[0]['val1'],
                            str(filter1.iloc[0]['kurs1']),
                            filter1.iloc[0]['Vol2'],
                            filter1.iloc[0]['val2'],
                            filter1.iloc[0]['Vol3'],
                            filter1.iloc[0]['val2'],
                            str(filter1.iloc[0]['kurs2']),
                            filter1.iloc[0]['Vol4'],
                            filter1.iloc[0]['val3'])
            Balance.NewBalance()
            # file = open(second_path_data + "\\new_regims.json", "r")
            # data = json.load(file)
            # file.close()
            # for k, v in data.items():
            #     v['avtomat'] = 'off'
            #
            # f = open(second_path_data + "\\new_regims.json", "w")
            # json.dump(data, f)
            # f.close()
            time.sleep(1.1)
        else:
            pass
    else:
        dw2 = {'regim': 'пусто',
               'timed': 'пусто',
               'b1': 'пусто',
               'b2': 'пусто',
               'val1': 'пусто',
               'val2': 'пусто',
               'val3': 'пусто',
               'kurs1': 0,
               'kurs2': 0,
               'Vol1': 0,
               'Vol2': 0,
               'Vol3': 0,
               'Vol4': 0,
               'profit': 0,
               'Go': 'пусто',
               }
        df2 = pd.DataFrame(data=dw2, index=[0])
        df2.drop_duplicates(inplace=True)
        df2.to_csv(second_path_data + "\\vilki.csv", header=True, index=False)


    return

if __name__ == "__main__":
    while True:
        try:
            t1 = time.time()
            fast_refresh()
            t2 = time.time()
            print("ALL TIME :", t2-t1)
            time.sleep(0.1)

        except Exception as e:
            print(e)
            time.sleep(0.2)
