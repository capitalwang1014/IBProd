'''
Created on Jun 4, 2017

@author: wang
'''

# for IB data pull
from ib.opt import ibConnection, message
from ib.ext.Contract import Contract
from ib.ext.Order import Order


# from Tool.ibinterface import ibConnection, message, Contract,Order

import pickle
# import Tool.ModelEngine as ME
import sys
# sys.path.append('~workspace/Production/Tools/')
import Tools.MaxDESlim as DE

import importlib
importlib.reload(DE)
# import Tool.ModelEngine as ME
import numpy as np
import pandas as pd
# from scipy.special import logit 
from scipy.special import expit
import math
import sys
from random import randint
from time import sleep
import datetime
import time
import urllib
from bs4 import BeautifulSoup
# from pandas.tests.io.parser import index_col
from pandas_datareader import data as pdr

import fix_yahoo_finance as yf
yf.pdr_override() # <== that's all it takes :-)


# from DailyData import get_current_index

'''
mystery function
'''
def gen_tick_id():
    i = randint(100, 10000)
    while True:
        yield i
        i += 1
if sys.version_info[0] < 3:
    gen_tick_id = gen_tick_id().next
else:
    gen_tick_id = gen_tick_id().__next__

'''
utility
'''

def myceil(x, prec=2, base=0.05):
    return float("{0:.2f}".format(base * np.ceil(float(x)/base)))
   
def myfloor(x, prec=2, base=0.05):
    return float("{0:.2f}".format(base * np.floor(float(x)/base)))    
    
def sleepto(to_hour,to_min,to_sec):

#     from time import sleep  
    now_time = datetime.datetime.now()
    target_time = datetime.datetime(now_time.year, now_time.month, now_time.day, to_hour, to_min, to_sec)
    if (target_time-now_time).total_seconds() < 0:
        target_time += datetime.timedelta(days=1)
         
    sleep_sec = (target_time-now_time).total_seconds()
    print('sleep : ',sleep_sec,'  sec')
    sleep(sleep_sec)

def ini_log(final_pick):
    
    path =  '/home/wang/Dropbox/Stockbet/Data/Projects/Simu/'
    Datestr = datetime.date.today().strftime("%Y%m%d")
    logact = open(path + 'Newlog'+Datestr+'.txt', 'w+')
    if len(final_pick)>0:
        picks = '!'.join(final_pick)
    else:
        picks = 'no pick today'
    logact.write("picked:"+picks+"\n")
    logact.close()
    
def log_act(txt):
    path =  '/home/wang/Dropbox/Stockbet/Data/Projects/Simu/'
    Datestr = datetime.date.today().strftime("%Y%m%d")
    logact = open(path + 'Newlog'+Datestr+'.txt', 'a')
    logact.write(txt+"\n")
    logact.close()
    
'''
index price pull
'''
def tag2price(tag):
#     li_t = '<li style="width:30.6%;" class=" D(ib) Bxz(bb) Bdc($c-fuji-grey-c)  Mend(16px)  BdEnd " aria-label="S&amp;P 500" data-reactid="14"><h3 class="Maw(160px)" data-reactid="15"><a class="Fz(s) Ell Fw(b) C($c-fuji-blue-1-b)" href="/quote/^GSPC?p=^GSPC" title="S&amp;P 500" aria-label="S&amp;P 500 has increased by 0.17% or 4.65 points to 2,727.72 points" data-reactid="16">S&amp;P 500</a><br data-reactid="17"/><span class="Trsdu(0.3s) Fz(s) Mt(4px) Mb(0px) Fw(b) D(ib)" data-reactid="18">2,727.72</span><div class="Fz(xs) Fw(b)  C($dataGreen)" data-reactid="19"><span class="Trsdu(0.3s)  C($dataGreen)" data-reactid="20">+4.65</span><span class="Mstart(2px)" data-reactid="21"><!-- react-text: 22 -->(<!-- /react-text --><span class="Trsdu(0.3s)  C($dataGreen)" data-reactid="23">+0.17%</span><!-- react-text: 24 -->)<!-- /react-text --></span></div><a target="_blank" rel="noopener" class="Fl(end) Mt(-30px)" href="/chart/^GSPC" data-symbol="^GSPC" title="S&amp;P 500 Chart" data-reactid="25"><canvas style="width:70px;height:25px;" data-reactid="26"></canvas></a></h3></li>'
#     tag = BeautifulSoup(li_t,"lxml")
    spans = tag.find_all('span')
    price, price_chg,price_chg_pct = 0,0,0
    for span in spans:
        if span.string is not None:
            print(span.string)
            if '+' in span.string or '-' in span.string:
                if '%' in span.string:
                    price_chg_pct = float(span.string.replace(',', '').replace('%',''))/100.0
                else:
                    price_chg = float(span.string.replace(',', ''))
                    
                
                print('price change',price_chg)
            else:
                price =float(span.string.replace(',', ''))
                
    return price, price_chg, price_chg_pct
def treatment(soj,std,treat_num = 3):
    pick = False
    if treat_num == 1:
        if soj >0 and std >-0.01:
            pick = True
    elif treat_num == 2:    
        if soj >0 and std >-0.004:
            pick = True
    elif treat_num == 3:
        if soj >0 and std >-0.003:
            pick = True
    elif treat_num == 4:
        if soj >-0.01 and std >-0.01:
            pick = True
    elif treat_num == 5:
        if soj >-0.01 and std >-0.005:
            pick = True
    elif treat_num == 6:
        if std >-0.005:
            pick = True
    
    elif treat_num == 7:
        if std >-0.012:
            pick = True
    elif treat_num == 8:
        pick = False
    
    return pick
    

def pick_rule(moj,soj,mtd,std):
    #def grid
    if moj >0.006:
        pick = treatment(soj, std, treat_num = 1)
    elif moj >0.005:
        if mtd>-0.002:
            pick = treatment(soj, std, treat_num = 2)
        else:
            pick = treatment(soj, std, treat_num = 3)
    elif moj >0.004:
        if mtd>0.002:
            pick = treatment(soj, std, treat_num = 2)
        else:
            pick = treatment(soj, std, treat_num = 3)
    elif moj >0.003:
        pick = treatment(soj, std, treat_num = 3)
    elif moj >0.002:
        if mtd>-0.001: #modified from -0.001 to -0.002 on the 7/18 data
            pick = treatment(soj, std, treat_num = 4)
        else:
            pick = treatment(soj, std, treat_num = 5)
        
    elif moj >0.001:
        if mtd>-0.002:
            pick = treatment(soj, std, treat_num = 4)
        else:
            pick = treatment(soj, std, treat_num = 5)
            
    elif moj >-0.001:
        if mtd>-0.002: #modified from -0.001 to -0.002 on the 7/17 data
            pick = treatment(soj, std, treat_num = 4)
        else:
            pick = treatment(soj, std, treat_num = 5)
    elif moj >-0.002:
        if mtd>0.001:
            pick = treatment(soj, std, treat_num = 4)
        else:
            pick = treatment(soj, std, treat_num = 5)
    elif moj >-0.003:
        if mtd>0.001:
            pick = treatment(soj, std, treat_num = 4)
        else:
            pick = treatment(soj, std, treat_num = 5)
    elif moj >-0.004:
        if mtd>0.001:
            pick = treatment(soj, std, treat_num = 7)
        else:
            pick = treatment(soj, std, treat_num = 6)
    elif moj >-0.005:
        if mtd<-0.002:
            pick = treatment(soj, std, treat_num = 8)
        elif mtd < 0.001:
            pick = treatment(soj, std, treat_num = 6)
        else :
            pick = treatment(soj, std, treat_num = 7)
    elif moj >-0.006:
        if mtd<-0.001:
            pick = treatment(soj, std, treat_num = 8)
        elif mtd < 0.001:
            pick = treatment(soj, std, treat_num = 6)
        else :
            pick = treatment(soj, std, treat_num = 7)
    elif moj >-0.007:
        if mtd<0.001:
            pick = treatment(soj, std, treat_num = 8)
        else :
            pick = treatment(soj, std, treat_num = 7)
    elif moj <-0.007:
        if mtd<-0.001:
            pick = treatment(soj, std, treat_num = 8)
        else :
            pick = treatment(soj, std, treat_num = 7)
    return pick
'''
test pick
'''
# for i in range()
# 
# pick = pick_rule(moj=0.0045,soj=-0.001,mtd=-0.00123,std = -0.0035)
# pick
def get_current_index():
    #get Yahoo main page
    SP500_price,SP500_price_chg,SP500_price_chg_pct,Dow30_price,Dow30_price_chg,Dow30_price_chg_pct,Nasdaq_price,Nasdaq_price_chg,Nasdaq_price_chg_pct = 0,0,0 ,0,0,0 ,0,0,0
    try:
        with urllib.request.urlopen('https://finance.yahoo.com/') as response:
            html = response.read()

    except:
        print('web page get failed')
        html =  ''
    #parse to get index price
    if html != '':
        
#         li_t = '<li style="width:30.6%;" class=" D(ib) Bxz(bb) Bdc($c-fuji-grey-c)  Mend(16px)  BdEnd " aria-label="S&amp;P 500" data-reactid="14"><h3 class="Maw(160px)" data-reactid="15"><a class="Fz(s) Ell Fw(b) C($c-fuji-blue-1-b)" href="/quote/^GSPC?p=^GSPC" title="S&amp;P 500" aria-label="S&amp;P 500 has increased by 0.17% or 4.65 points to 2,727.72 points" data-reactid="16">S&amp;P 500</a><br data-reactid="17"/><span class="Trsdu(0.3s) Fz(s) Mt(4px) Mb(0px) Fw(b) D(ib)" data-reactid="18">2,727.72</span><div class="Fz(xs) Fw(b)  C($dataGreen)" data-reactid="19"><span class="Trsdu(0.3s)  C($dataGreen)" data-reactid="20">+4.65</span><span class="Mstart(2px)" data-reactid="21"><!-- react-text: 22 -->(<!-- /react-text --><span class="Trsdu(0.3s)  C($dataGreen)" data-reactid="23">+0.17%</span><!-- react-text: 24 -->)<!-- /react-text --></span></div><a target="_blank" rel="noopener" class="Fl(end) Mt(-30px)" href="/chart/^GSPC" data-symbol="^GSPC" title="S&amp;P 500 Chart" data-reactid="25"><canvas style="width:70px;height:25px;" data-reactid="26"></canvas></a></h3></li>'
        soup = BeautifulSoup(html,"lxml")
        lis = soup.find_all('li')

        for li_t in lis:
            if li_t.attrs.get('aria-label') == 'S&P 500':
#                 print('find SP500')
                SP500_price,SP500_price_chg,SP500_price_chg_pct = tag2price(li_t)
            if li_t.attrs.get('aria-label') == "Dow 30":
#                 print('find Dow 30')
                Dow30_price,Dow30_price_chg,Dow30_price_chg_pct = tag2price(li_t)
                
            if li_t.attrs.get('aria-label') == "Nasdaq":
#                 print('find Nasdaq')
                Nasdaq_price,Nasdaq_price_chg,Nasdaq_price_chg_pct = tag2price(li_t)
                
    return SP500_price,SP500_price_chg,SP500_price_chg_pct,Dow30_price,Dow30_price_chg,Dow30_price_chg_pct,Nasdaq_price,Nasdaq_price_chg,Nasdaq_price_chg_pct
    
    
'''
message handler
'''

def save_order_id(msg):
    print(msg)
    trdcontrol.order_ids = msg.orderId
    
def account_value(msg):
#     print(msg)
    if msg.key == 'NetLiquidation' and msg.currency == 'USD':
#         print('in balance')
        trdcontrol.netLiquidation = float(msg.value)
    if msg.key == 'TotalCashValue' and msg.currency == 'USD':
#         print('cash in')
        trdcontrol.cashbalance = float(msg.value)
#         for s_type in strategyLookup.keys():
#             strategyLookup[s_type].loc[0,'CashBalance'] = cashbalance
    if msg.key == 'UnrealizedPnL' and msg.currency == 'USD':
#         print('cash in')
        trdcontrol.UnrealizedPnL = float(msg.value)

def market_price(msg):
    
    tickerID = int(msg.tickerId)
    symbol_t = trdcontrol.tick2symbol[tickerID]
    changed = False
#     print(symbol_t, msg)
    trdcontrol.smartlist[symbol_t].snap_shot.datetime_t =  datetime.datetime.now()    
    if hasattr(msg, 'price'):   
#         print('price type:',type(msg.price)) 
        if msg.field == 1:
            if float(trdcontrol.smartlist[symbol_t].snap_shot.Bid) != msg.price:
#                 changed = True
#                 print(symbol_t,'BID',msg.price)
                trdcontrol.smartlist[symbol_t].snap_shot.Bid = msg.price
                if msg.price > trdcontrol.smartlist[symbol_t].snap_shot.Last:
                    changed = True 
                    trdcontrol.smartlist[symbol_t].snap_shot.Last = msg.price
                
        elif msg.field == 2:
            if float(trdcontrol.smartlist[symbol_t].snap_shot.Ask) != msg.price:
#                 changed = True            
#                 print(symbol_t,'ASK',msg.price)
                trdcontrol.smartlist[symbol_t].snap_shot.Ask = msg.price
                if msg.price < trdcontrol.smartlist[symbol_t].snap_shot.Last:
                    changed = True 
                    trdcontrol.smartlist[symbol_t].snap_shot.Last = msg.price     
                             
        elif msg.field == 4:
            if float(trdcontrol.smartlist[symbol_t].snap_shot.Last) != msg.price:
                changed = True     
#                 print(symbol_t,'LAST',msg.price)        
                trdcontrol.smartlist[symbol_t].snap_shot.Last = msg.price
#                 if (msg.price > 0) & (msg.price < trdcontrol.smartlist[symbol_t].snap_shot.Allmin) &(time_t > minstarttime):
#                     trdcontrol.smartlist[symbol_t].snap_shot.Allmin = msg.price
        elif msg.field == 6:
            trdcontrol.smartlist[symbol_t].snap_shot.High = msg.price
        elif msg.field == 7:
            trdcontrol.smartlist[symbol_t].snap_shot.Low = msg.price            
        elif msg.field == 9:
            if float(trdcontrol.smartlist[symbol_t].snap_shot.Close) != msg.price:
#                 changed = True             
                trdcontrol.smartlist[symbol_t].snap_shot.Close = msg.price
    elif hasattr(msg, 'size'):
#         print('size type:',type(msg.size)) 
        if msg.field == 0:
#             print(symbol_t,'BIDsize',msg.size)
            trdcontrol.smartlist[symbol_t].snap_shot.BidVol = msg.size
        elif msg.field == 3:
            trdcontrol.smartlist[symbol_t].snap_shot.AskVol = msg.size
#             print(symbol_t,'Asksize',msg.size)
        elif msg.field == 5:
            if int(trdcontrol.smartlist[symbol_t].snap_shot.LastVol) != msg.size:
#                 changed = True
#                 print(symbol_t,'Lastsize',msg.size)            
                trdcontrol.smartlist[symbol_t].snap_shot.LastVol = msg.size
        elif msg.field == 8:
            if int(trdcontrol.smartlist[symbol_t].snap_shot.Volume) != msg.size:
#                 changed = True
                trdcontrol.smartlist[symbol_t].snap_shot.Volume = msg.size
            
    if changed : #when last price change there may be some actions 
#         symbol_t = 'AAA'
#         print('got data:',symbol_t,datetime.datetime.now())
        trdcontrol.smartlist[symbol_t].add_data()              
        


def order_status(msg):
    print(msg)
    order_id = msg.orderId
    print('order msg:',msg.status,'order id:',msg.orderId)
    for symbol, smart_ib in trdcontrol.smartlist.items():
        if order_id in smart_ib.trade_stat.buyorderstatus.keys():
            smart_ib.trade_stat.buyorderstatus[order_id] = msg.status
            
        elif order_id in smart_ib.trade_stat.sellorderstatus.keys():
            smart_ib.trade_stat.sellorderstatus[order_id] = msg.status
        
#     if msg.status == 'Filled':
#         con.reqAccountUpdates(gen_tick_id(), '')
        
def portfolio(msg):
    print(msg)
#     global symbolStatus
#     if msg.position > 0:
#         symbol_t = msg.contract.m_symbol
#         symbolStatus[symbol_t].iloc[0,4] = msg.position
#         symbolStatus[symbol_t].iloc[0,5] = msg.averageCost
#         symbolStatus[symbol_t].iloc[0,6] = msg.averageCost*msg.position
        
        
def position(msg):
    print(msg)
    
    symbol_t = msg.contract.m_symbol
    #record position data
    if symbol_t in trdcontrol.smartlist.keys():                 
        trdcontrol.smartlist[symbol_t].trade_stat.hold_volume = msg.pos
        trdcontrol.smartlist[symbol_t].trade_stat.avg_price = msg.avgCost
        trdcontrol.smartlist[symbol_t].trade_stat.tot_cost = msg.avgCost*msg.pos


def account_summary(msg):
#     print(account)
    print(msg)    


class TradeControl:
    '''
    global control of all concurrent the trade
    '''
    
    def __init__(self,budget):
        self.order_ids = 0
        self.con = ibConnection()
        self.models  = []
        self.symbol_list = pd.DataFrame()
        self.netLiquidation = 0
        self.cashbalance = 0
        self.UnrealizedPnL = 3.1415926
        self.moneyAvailable = 0
        self.moneyInplay = 13495.0
        self.buyPower = 90000.00
        self.concurrent_ratio = 0.28
        
        self.tick2symbol = {}
        self.smartlist = {}
        self.tot_profit = 0
        self.acm_profit = budget
        #initiatelize the Dow dictionary
        self.DJI_dic = {}
        symbol_path = '/home/wang/Documents/Production/DaliyModel/Symbollist/'
        DJI30_df = pd.read_csv(symbol_path +'DJI30.csv')
        for symbol in DJI30_df.Symbol:
            self.DJI_dic[symbol] = 0
        self.DJI_cal = 0
        self.DJI_multi =6.7805465760539
    
        
                
    def next_order_id(self):
        self.order_ids +=1
        return self.order_ids

    def makeconnection(self):
        self.con.register(account_value, 'UpdateAccountValue')
        self.con.register(market_price, 'TickPrice', 'TickSize')
        self.con.register(save_order_id, 'NextValidId')
    #     con.register(portfolio, 'UpdatePortfolio')
        self.con.register(position, 'Position')
        self.con.register(order_status,'OrderStatus')
        self.con.register(account_summary,'AccountSummary')
        self.con.connect()

    def get_account_value(self):
        print('get account value')
        self.con.reqAccountUpdates(1, '')
        
        while self.netLiquidation == 0.0 or self.UnrealizedPnL==3.1415926: #wait for account_value to update cashbalance
            sleep(0.1)
#         print(UnrealizedPnL)
            
        self.moneyAvailable = self.netLiquidation-self.UnrealizedPnL
        
    def request_position(self):
        self.con.reqPositions()
        sleep(10)
        
    def request_market_data(self):
            
        
        #wait for all the positions has been updated
        #request market data
        print('request market data')
        for symbol_t,smart_ib in self.smartlist.items():
            tickID = smart_ib.trade_stat.tick_id
            contract = smart_ib.trade_stat.contract
            self.con.reqMktData(int(tickID), contract, '', False)    

    
    def init_smartlist(self):
        symbols = list(self.symbol_list.index)
        for symbol in symbols:
            smart_ib = Smart_IB(symbol)
            smart_ib.trade_stat.moneyplay = (self.symbol_list.loc[symbol,'moneyplay'] == 1)
#             smart_ib.trade_stat.moneyplay = False
            smart_ib.trade_stat.mintrade = self.symbol_list.loc[symbol,'mintrade']
            smart_ib.trade_stat.kelly_pct = self.symbol_list.loc[symbol,'kelly_0lose']
            self.smartlist[symbol] = smart_ib
            
            self.tick2symbol[smart_ib.trade_stat.tick_id] = symbol
            print('load: ',symbol)
            
    def init_trade_size(self,inplay):
#         inplay = self.symbol_list.moneyplay.sum()
        
#         self.margin_buget = 0
        self.trade_size_high =  max(self.moneyInplay/5,8000)
        self.trade_size_low =  max(self.moneyInplay/6,6000)
        self.margin_buget = self.moneyInplay - inplay*self.concurrent_ratio * self.trade_size_low
        self.margin_buget = max(-self.buyPower+self.trade_size_low
                                ,self.margin_buget)
#         self.margin_buget = 500
        print('margin:',self.margin_buget,'max_size:',self.trade_size_high,'min_size:',self.trade_size_low)
            
    
    def para_prediction(self,row,model_index):
        GLMparameters = self.models[model_index][0]
        GLMparaoffset = self.models[model_index][1]
        #score data set by the para dataset
        #find varname from GLM parameter list
        test_pred = 0
        for i, varpara in enumerate(GLMparameters):
            varname = list(varpara.columns.values)[0]
#             print('varname:',varname,'  value:',row[varname])
            def get_score(value):
                values = varpara[varname]
                idx = (np.abs(values-value)).argmin()
                return varpara.loc[idx,'logit']
            test_pred += get_score(row[varname])
        
        return expit(test_pred +  GLMparaoffset)
    
    def load_model(self,model_pkl):
        output = open(model_pkl, 'rb')
        BestModel=pickle.load(output)
        output.close()
        self.models.append(BestModel)
        
    def save_data(self,path):
        Datestr = datetime.date.today().strftime("%Y%m%d")
        for symbol,smartib in self.smartlist.items():
            file_t = path + symbol + Datestr + '.csv'
            smartib.d_engine.rawdata.to_csv(file_t)
#             file_model = path + symbol + Datestr + 'Model.csv'
#             smartib.d_engine.model_trend_output.to_csv(file_model)
#             file_stat = path + symbol + Datestr + 'trend.csv'
#             smartib.d_engine.trend_stat.to_csv(file_stat)
    def close_sale(self):
        
        
        #gather all the holding position without sale flag
        symbol_close=[]
        vol_hold = []
        avg_price = []
        cur_price = []
        moneyplays = []
        for symbol_t,smart_ib in self.smartlist.items():
            if (smart_ib.trade_stat.hold_volume>0 and 
#                 smart_ib.trade_stat.moneyplay and 
                smart_ib.trade_stat.buysellstatus != 'ToBuy'):
                 
                print('symbol in pain:',symbol_t)
                vol_hold.append(smart_ib.trade_stat.hold_volume)
                avg_price.append(smart_ib.trade_stat.avg_price)
                symbol_close.append(symbol_t)
                cur_price.append(smart_ib.snap_shot.Last) 
                moneyplays.append(smart_ib.trade_stat.moneyplay)
                
        if len(symbol_close)>0:
            
            holds_df_all = pd.DataFrame({'hold_vol':vol_hold,
                                     'avg_price':avg_price,
                                     'cur_price':cur_price,
                                     'moneyplay':moneyplays},index = symbol_close)
            
            holds_df = holds_df_all[holds_df_all.moneyplay] 
            holds_df_simu = holds_df_all[~holds_df_all.moneyplay]
            if holds_df.shape[0]>0: 
                holds_df['pctdrop'] = holds_df.cur_price/holds_df.avg_price
                holds_df['money_loss'] = (holds_df.cur_price-holds_df.avg_price)*holds_df.hold_vol
                holds_df = holds_df.sort_values(by=['pctdrop'],  ascending = False)
                
                holds_df['acm_loss']= holds_df.money_loss.cumsum()
                
                pain_payment = self.acm_profit  #pre-seted buget for payment
        #             pain_payment = (38+10)/2
                holds_df['close_ind'] =  holds_df['acm_loss'].map(lambda x: 1 if pain_payment  > -x else 0)
                
                close_df = holds_df[holds_df['close_ind']== 1] #when profit can cover loss put in close df
                close_simu_df = holds_df[holds_df['close_ind']!= 1]
                if close_df.shape[0]>0:
                    #sale the stok in pain
                    for symbol_tt in close_df.index:
                        if self.smartlist[symbol_tt].trade_stat.buysellstatus != 'ToBuy':
                            self.smartlist[symbol_tt].close_sale_ind= True
                            sel_vol = self.smartlist[symbol_tt].trade_stat.hold_volume
                            sel_price = self.smartlist[symbol_tt].snap_shot.Last
                            self.smartlist[symbol_tt].sell_IB(sel_price,sel_vol)
                            
        #                     print('sel:',symbol,sel_vol,'at:',sel_price)
                else: #when profit can not cover even loss from one stock
                    if pain_payment >0 :
                        symbol = holds_df.index[0]
                        
                        sel_vol = int(abs(pain_payment/
                                          (holds_df.loc[symbol,'cur_price']-holds_df.loc[symbol,'avg_price'])
                                          )
                                      )
                        sel_price = holds_df.loc[symbol,'cur_price']
                        
                        if sel_vol>0:
                            self.smartlist[symbol].sell_IB(sel_price,sel_vol)
                        
                    #simu sale the retained moneyplay
                if close_simu_df.shape[0]>0:
                    for symbol_tttt,row in close_simu_df.iterrows():
                        self.smartlist[symbol_tttt].sell_simu(row['cur_price'],row['hold_vol'])    
        #                 print('sel:',symbol,sel_vol,'at:',sel_price)
        
            #sale all simu
            for symbol_ttt,row in holds_df_simu.iterrows():
                self.smartlist[symbol_ttt].sell_simu(row['cur_price'],row['hold_vol'])
                
                
#             print(holds_df)  
            #sale all the profit covered stock      
#             for symbol_t,smart_ib in self.smartlist.items():
#                 if (smart_ib.trade_stat.hold_volume>0 and 
#                     
#                     smart_ib.trade_stat.buysellstatus != 'ToBuy' and 
#                     smart_ib.close_sale_ind
#                     ):
#                     
                    
    def profit_report(self,path):
        #file name
        Datestr = datetime.date.today().strftime("%Y%m%d")
        file_t = path + 'profitRPT' + Datestr + '.csv'
        symbols = []
        profits_pct = []
        profits = []
        for symbol,smartib in self.smartlist.items():
            symbols.append(symbol)
#             if smartib.trade_stat.hold_volume>0:
#                 profit_pct_t = -0.01
#                 profit_t = -99
#             else:
            profit_pct_t = smartib.trade_stat.trade_profit_pct
            profit_t = smartib.trade_stat.trade_profit
            
            profits_pct.append(profit_pct_t)
            profits.append(profit_t)
            
        profit_pct_df = pd.DataFrame({'Symbol':symbols,Datestr:profits_pct})
        profit_pct_df.to_csv(file_t)
        
        profit_df = pd.DataFrame({'Symbol':symbols,Datestr:profits})
#         profit_df.to_csv(file_t)
        
        profit_df = profit_df.sort_values(by=[Datestr])
        print(profit_df)
        on_hold = profit_df[profit_df[Datestr] == -99]
        solds  = profit_df[profit_df[Datestr] != -99]
        print('On_hold:', on_hold.shape[0],'\n'
              'Profits:',solds[Datestr].sum())
        
    
class Trade:
    '''
    individual symbol trade management
    '''
    def __init__(self,symbol_t):
        self.symbol = symbol_t
        self.mintrade = 0.01
        self.tick_id = gen_tick_id()
        self.basefund = 4000
        self.kelly_pct = 0.2
        self.buysellstatus = 'ToBuy' #'BuySubmitted' 'ToSell' 'SellSubmitted' 'ToBuy'
        self.buyorderstatus = {}  #a dictionary of order id to status
        self.buyordercost = {} #a dictionary of order id to cost
        self.sellorderstatus = {} #a dictionary of order id to status
        self.hold_volume = 0
        self.avg_price = 0.0
        self.last_buy = 0.0
        self.tot_cost = 0.0
        self.postbuy_low = 9999.99
        self.postbuy_high = 0.0
        self.trade_profit = 0
        self.trade_profit_pct = 0.0
        self.contract  = self.make_contract()
        self.buytime_min = 0.0
        
#         self.buyprice = 0
        self.buysettime = 0
#         self.sellprice = 9999
        self.sellsettime = 0
        self.moneyplay = False
        
    def TSupdate(self,snap_shot):
        if self.hold_volume > 0:
            if snap_shot.Last <self.postbuy_low:
                self.postbuy_low = snap_shot.Last
            if snap_shot.Last > self.postbuy_high:
                self.postbuy_high = snap_shot.Last
    
    def make_contract(self):
        contract = Contract()
        contract.m_symbol = self.symbol
        contract.m_secType = 'STK'
        contract.m_exchange = 'SMART'
        contract.m_primaryExch = 'SMART'
        contract.m_currency = 'USD'
        contract.m_localSymbol = self.symbol
        return contract
    
    def make_order(self,limit_price,Quantity,Action):
        order = Order()
    #     order.m_minQty = 100
        order.m_lmtPrice = limit_price
        order.m_orderType = 'LMT'
        order.m_totalQuantity = Quantity
        order.m_action = Action #'SELL'/'BUY'
        
        return order

    def track_order_id(self,id_t,Tradetype,ordercost):
        if Tradetype == 'BUY':
            self.buyorderstatus[id_t] = 'Submitted'
            self.buyordercost[id_t] = ordercost
        elif Tradetype == 'SELL':
            self.sellorderstatus[id_t] = 'Submitted'
                
    def place_500(self,Price,Vol,Tradetype):
    
#     contract = symbolTrack.loc[symbol_t,'Contracts'] 
        Price = float("{0:.2f}".format(Price))
        print('price:',Price,'vol:',Vol)
        Nsells = math.ceil(Vol/500.0)
        if Nsells > 1.0:
            sigvol = math.floor(Vol/Nsells);
            for i in range(Nsells-1):
                id_t = trdcontrol.next_order_id()
                order = self.make_order(Price,sigvol,Tradetype)
                trdcontrol.con.placeOrder( id_t, self.contract, order )
                
                self.track_order_id(id_t,Tradetype,Price*sigvol) #save order ID
                
                
            finalvol = Vol - sigvol*(Nsells - 1)
            id_t = trdcontrol.next_order_id()
            order = self.make_order(Price,finalvol,Tradetype)
            
            self.track_order_id(id_t,Tradetype,Price*finalvol) #save order ID
            
            trdcontrol.con.placeOrder( id_t, self.contract, order )
        else:
            id_t = trdcontrol.next_order_id()
            order = self.make_order(Price,Vol,Tradetype)
            
            trdcontrol.con.placeOrder( id_t, self.contract, order )
         
            self.track_order_id(id_t,Tradetype,Price*Vol) #save order ID
            
    def cancel_orders(self,Tradetype):
        if Tradetype =='BUY':
            for ord_id,status in self.buyorderstatus.items():
                if status == 'Submitted':
                    trdcontrol.con.cancelOrder(ord_id)
                    #release fund to pool
                    trdcontrol.moneyInplay += self.buyordercost[ord_id]
                    
                    log_act('Cancel BUY:' + self.symbol + ' release:'+ str(self.buyordercost[ord_id]))
                    
        if Tradetype =='SELL':
            for ord_id,status in self.sellorderstatusj.items():
                if status == 'Submitted':
                    trdcontrol.con.cancelOrder(ord_id)
                    log_act('Cancel SELL:' + self.symbol )
    

class Box:
    def __init__(self,box_with = 120,tofirm_time = 80,tofirm_profit = 0.01):
        self.active = False
        self.buy_price = 0
        self.box_status = 'ToBeFirm'
        self.box_with = box_with
        self.tofirm_time = tofirm_time
        self.tofirm_profit =tofirm_profit
        self.speed_limit = 0.001
        self.base_line = self.buy_price
        self.data_df = pd.DataFrame(columns=['Last'])
        self.break_time = datetime.datetime.now()
        
        self.ever_max = pd.DataFrame(columns=['Max'])
        self.min_post_max = pd.DataFrame(columns=['Min'])
        self.post_max_df = pd.DataFrame(columns=['Last'])
        
        self.quick5 = False
        self.quick5time = 80
        self.mono_increase = 0
        
        self.box_min = 0
        self.all_time_high = 0
        
        self.max_track = {'break_life':0
                          ,'max':0
                          ,'max_last_time':0
                          ,'post_max_min':0
                          ,'post_max_min_drop':0
                          ,'post_min_time':0
                          ,'new_max_inc':0
                          ,'max_speed':0
                          ,'last_max':False}
        self.max_df = pd.DataFrame(columns = ['break_life'
                          ,'max'
                          ,'max_last_time'
                          ,'post_max_min'
                          ,'post_max_min_drop'
                          ,'post_min_time'
                          ,'new_max_inc'
                          ,'max_speed'
                          ,'last_max'])
        
    def break_out(self,snap_shot,avg_price):
        self.active = True
        self.buy_price = avg_price
        self.base_line = self.buy_price *1.001
        #reset data_df
        self.data_df = pd.DataFrame(columns=['Last'])
        self.data_df.loc[snap_shot.datetime_t] = [snap_shot.Last]
        self.break_time = snap_shot.datetime_t
        print('base line:',self.base_line)
        self.ever_max = pd.DataFrame(columns=['Max'])
        self.ever_max.loc[snap_shot.datetime_t] = snap_shot.Last
        
        self.min_post_max = pd.DataFrame(columns=['Min'])
        self.min_post_max.loc[snap_shot.datetime_t] = snap_shot.Last
        
        self.post_max_df = pd.DataFrame(columns=['Last'])
        
        self.box_min = 0
        self.all_time_high = 0
        self.mono_increase = 0
        self.quick5 = False
        
        
    def decision(self,snap_shot):

        break_life = (snap_shot.datetime_t - self.break_time).total_seconds()
        if self.ever_max.shape[0]>0:
            
            max_life = (snap_shot.datetime_t - self.ever_max.index[-1]).total_seconds()
        else :
            max_life = 0
            
        speed = np.abs((snap_shot.Last/self.data_df.iloc[-1]['Last'] - 1)/(snap_shot.datetime_t-self.data_df.index[-1]).total_seconds())
        
        action = ''
        #always sell when price bellow base line if above firm time
        if speed <self.speed_limit:
            if snap_shot.Last <= self.base_line:
                if break_life <self.tofirm_time:
                    if self.quick5:
                        action = 'sale'
#                         debug['sold_type'] = 'Q5earlyBase'
                    else:
                        action = 'restart'
                else:
                    action = 'sale'
#                     debug['sold_type'] = 'Base_line'
            #collect the unpromissing bellow 0.5% 
            elif break_life > self.quick5time and self.quick5 == False and self.all_time_high < self.buy_price*1.005:
                
                if (max_life > 120 and 
                    snap_shot.Last >  ((self.all_time_high -self.buy_price)*0.75 + self.buy_price) and 
                    self.min_post_max.shape[0]>1 and 
                    self.post_max_df.shape[0]>4):
                    action = 'sale'
#                     debug['sold_type'] = 'Max60later'
#                     print('sold type:','Max60later'
#                           ,' max_life:',max_life
#                           ,' Last:',snap_shot.Last
#                           ,' all time high:',self.all_time_high
#                           , ' buy price:', self.buy_price
#                           ,'break life:',break_life
#                           ,'quick 5:', self.quick5)
                    
#                 if snap_shot.Last > self.ever_max.iloc[-1]['Max']:
#                     max_speed = (snap_shot.Last/self.ever_max.iloc[-1]['Max'] - 1)/max_life
#                     if max_speed < 0.001/120:
#                         action = 'sale'
#                         debug['sold_type'] = 'Maxslow'
                        
                    #record the max track
                     
                           
                    
                if self.mono_increase > 6 and snap_shot.Last < self.data_df.iloc[-1]['Last']:
                    action = 'sale'
#                     debug['sold_type'] = 'continuestop'
                 

#                 debug['sold_type'] = 'MarketClose'
        else:
            action = 'hold'
        
#         if action == 'sale':
            
            
#             debug['price_freq'] = self.data_df.shape[0]
#             debug['possibleMax'] = self.all_time_high/self.buy_price - 1
#             print('possible max:',debug['possibleMax'])
            
        
                 
        self.add_point(snap_shot)
        
#         if snap_shot.Last < self.buy_price*1.001:
#             action = 'track'
        
        return action
    
    def drop_check(self,snap_shot):
        out = ''
        speed = np.abs((snap_shot.Last/self.data_df.iloc[-1]['Last'] - 1)/(snap_shot.datetime_t-self.data_df.index[-1]).total_seconds())
#         print('snap_shot last:',snap_shot.Last,'data_df_last',self.data_df.iloc[-1]['Last'])
#         print('snap_shot time:',snap_shot.datetime_t,'data_df time',self.data_df.index[-1])
#         print('speed:',speed)
        if speed <self.speed_limit:
            if snap_shot.Last < self.base_line:
                out = 'sale'
                print('base_line sale:max profit',self.all_time_high/self.buy_price - 1)
#                 debug['sold_type'] = 'base_line'
#                 debug['price_freq'] = self.data_df.shape[0]
#                 debug['possibleMax'] = self.all_time_high/self.buy_price - 1
#                 
            else :
                if snap_shot.Last < self.box_min and self.data_df.shape[0]>4:
#                     out = 'sale'
                    out = 'hold'
#                     debug['price_freq'] = self.data_df.shape[0]
# #                     print('box_min sale:max profit',self.all_time_high/self.buy_price - 1)
#                     debug['sold_type'] = 'box_min'
#                     debug['possibleMax'] = self.all_time_high/self.buy_price - 1
#                     
                else:
                    out = 'hold'
                    
        else:
            out = 'hold'
            
#             print('speed hold')
        return out
                
    def add_point(self,snap_shot):
        
        break_life = (snap_shot.datetime_t - self.break_time).total_seconds()
        if self.ever_max.shape[0]>0:
            
            max_life = (snap_shot.datetime_t - self.ever_max.index[-1]).total_seconds()
            max_speed = (snap_shot.Last/self.ever_max.iloc[-1]['Max'] - 1)/max_life
        else :
            max_life = 0
            max_speed = 0
        #adjust the mono increase
        if self.data_df.shape[0]>0:
            if snap_shot.Last < self.data_df.iloc[-1]['Last']:
                self.mono_increase = 0
            else :
                self.mono_increase += 1
                
            
        self.data_df.loc[snap_shot.datetime_t] = snap_shot.Last
        #adjust data
        self.data_df = self.data_df[self.data_df.index >= (self.data_df.index[-1]-datetime.timedelta(seconds = self.box_with))]
        self.post_max_df.loc[snap_shot.datetime_t] = snap_shot.Last
#         print('box_with:',self.box_with,'data_df size:',self.data_df.shape[0])
        
        self.box_min = min(self.data_df['Last'])
        
        if snap_shot.Last >self.all_time_high:
            
            self.max_track['break_life']=break_life
            self.max_track['max'] = self.ever_max.iloc[-1]['Max']
            self.max_track['max_last_time']= max_life
            self.max_track['post_max_min'] = self.min_post_max.iloc[-1]['Min']
            self.max_track['post_max_min_drop']=self.min_post_max.iloc[-1]['Min']/self.ever_max.iloc[-1]['Max'] - 1
            self.max_track['post_min_time']=(self.min_post_max.index[-1]-self.ever_max.index[-1]).total_seconds()
            self.max_track['new_max_inc'] =snap_shot.Last/self.ever_max.iloc[-1]['Max'] - 1
            self.max_track['max_speed'] = max_speed
            
            self.max_df.loc[snap_shot.datetime_t] = self.max_track
            
            self.post_max_df = pd.DataFrame(columns=['Last'])   
            
            self.ever_max.loc[snap_shot.datetime_t] = snap_shot.Last
            
            
            
            self.min_post_max = pd.DataFrame(columns=['Min']) 
            self.min_post_max.loc[snap_shot.datetime_t] = snap_shot.Last
            
            if snap_shot.Last > self.buy_price*1.005 and break_life<self.quick5time:
                self.quick5 = True
                
            self.all_time_high = snap_shot.Last
            if self.all_time_high>self.buy_price*1.005:
                
                self.base_line =(max(self.all_time_high/self.buy_price -1.005,0.0025) + 1) * self.buy_price
#                 print('new base line:',self.base_line) 
#                 pass
#             print('all ttime high:',self.all_time_high,'  ',snap_shot.datetime_t)
            
        if self.min_post_max.shape[0] > 0:
            if snap_shot.Last <self.min_post_max.iloc[-1]['Min']:
                self.min_post_max.loc[snap_shot.datetime_t] = snap_shot.Last
        
        #update box
#         if self.box_status == 'ToBeFirm':
#             if (snap_shot.datetime_t - self.break_time).total_seconds()>self.tofirm_time or snap_shot.Last >self.buy_price *(1+self.tofirm_profit):
#                 self.box_status = 'Firmed'
#                 print('price firmed',snap_shot.Last, 'time:',snap_shot.datetime_t )
#         
        
        #make decision
    
            
class Smart_IB:
    '''
    complete handle of a single symbol
    '''
    def __init__(self,symbol_t):
        self.snap_shot = DE.PriceShot()
        self.d_engine = DE.MinTracker(symbol_t)
        self.box_with = 0
        self.tofirm_time = 80
        self.tofirm_profit = 0.01
        self.s_box = Box(box_with = self.box_with
                         ,tofirm_time = self.tofirm_time
                         ,tofirm_profit = self.tofirm_profit)
        self.trade_stat = Trade(symbol_t)
        self.rebuy_gap = 0.015
        self.max_track = pd.DataFrame()
        self.last_decision_time = datetime.datetime.now()
        self.close_sale_ind = False
        
        self.debug = True
    def set_box(self, box_with = 120,tofirm_time = 80,tofirm_profit = 0.01):
        self.box_with = box_with
        self.tofirm_time = tofirm_time
        self.tofirm_profit = tofirm_profit
        self.s_box = Box(box_with = box_with,tofirm_time = tofirm_time,tofirm_profit = tofirm_profit)
    
    def add_data(self):
        self.trade_stat.TSupdate(self.snap_shot)
        time_t, row = self.snap_shot.defill()
        self.d_engine.add_data_only(time_t,row)
        
    def decision(self):
        #take in data
        #only act on the new data point
        if self.last_decision_time < self.snap_shot.datetime_t:
            self.last_decision_time = self.snap_shot.datetime_t
            
            act = self.d_engine.check_action()
            #buy action on break
            if act :
                
                action = self.condition_check()
    #             print('action:',action)
                if action == 1:    
                    self.first_trend_buy(action)
                if action == 2:
                    self.first_trend_buy(action)
            #sell action 
            if self.trade_stat.hold_volume > 0 and self.trade_stat.buysellstatus != 'ToBuy':
    #             self.track_sell()
#                 self.box_sell()
    #             self.box_track()
                self.simple_sell()
            
            
                #if it is the first trend break and no action made
            #cancel order if price move 0.5% above or 0.5% bellow
            
            if (self.trade_stat.hold_volume ==0 and 
                self.trade_stat.buysellstatus == 'BuySubmitted' and 
                self.snap_shot.Last > self.trade_stat.last_buy*1.005):
                self.trade_stat.cancel_orders('BUY')
                self.trade_stat.buysellstatus = 'ToBuy'
                
            if (self.trade_stat.hold_volume >0 and 
                self.trade_stat.sellorderstatus == 'ToBuy' and 
                self.snap_shot.Last < self.trade_stat.sellprice * 0.995):
                
                self.trade_stat.cancel_orders('SELL')
                #reset sale
                sellvol =self.trade_stat.hold_volume 
                sellprice = myfloor(self.snap_shot.Last,2,self.trade_stat.mintrade)
                self.trade_stat.place_500(sellprice, sellvol, 'SELL')
    
                
                
    def rebuy_gap_sizeadj(self,size,gap):
        
        if size  < 50:
            adjgap = gap
        elif size < 150:
            adjgap = gap - 0.0045/100 *(size - 50)
        else:
            adjgap = gap - 0.0045
        return adjgap 
               
    def condition_check(self):
        stopbuytime = datetime.datetime(self.snap_shot.datetime_t.year, self.snap_shot.datetime_t.month, self.snap_shot.datetime_t.day, 11, 0, 0)
        stop2ndbuytime = datetime.datetime(self.snap_shot.datetime_t.year, self.snap_shot.datetime_t.month, self.snap_shot.datetime_t.day, 15, 30, 0)
        
        startbuytime = datetime.datetime(self.snap_shot.datetime_t.year, self.snap_shot.datetime_t.month, self.snap_shot.datetime_t.day, 9, 35, 0)
        if self.trade_stat.buysellstatus == 'ToBuy' and self.d_engine.modeldataB.shape[0] >= 1 and self.snap_shot.datetime_t < stopbuytime:
            return 1 #first buy
        adjgap = self.rebuy_gap_sizeadj(self.snap_shot.Last,self.rebuy_gap)
        
        if (self.trade_stat.hold_volume >0 and 
            self.d_engine.modeldataB.shape[0] >= 1 and 
            self.snap_shot.datetime_t < stop2ndbuytime and
            self.trade_stat.last_buy * (1-adjgap) > self.snap_shot.Last and
            (self.trade_stat.hold_volume * self.trade_stat.avg_price) < (2.5 *self.trade_stat.basefund)
            ):
            self.rebuy_gap *= 2
            return 2 #post first buy
        
    def box_track(self):
#         max_track = pd.DataFrame()
        action = ''
        if self.s_box.active:
            action = self.s_box.decision(self.snap_shot)
            if action =='restart':
                self.max_track = self.max_track.append(self.s_box.max_df)
                self.max_track.loc[self.max_track.index[-1],'last_max'] = True
                self.s_box.__init__(box_with = self.box_with,tofirm_time = self.tofirm_time,tofirm_profit = self.tofirm_profit)
                print('restart:df size-',self.s_box.data_df.shape[0])
            elif action == 'track':
                self.max_track = self.max_track.append(self.s_box.max_df)
                self.max_track.loc[self.max_track.index[-1],'last_max'] = True
                self.s_box.__init__(box_with = self.box_with,tofirm_time = self.tofirm_time,tofirm_profit = self.tofirm_profit)

            else:
                pass
        
        if self.snap_shot.Last >  self.trade_stat.avg_price*1.001 and self.s_box.active == False and action !='sale':
            print('break out ',self.snap_shot.datetime_t)
            self.s_box.break_out(self.snap_shot, self.trade_stat.avg_price)
#             debug['breaktimes']+= 1
        

#             self.debug = False
    def box_sell(self):
        action = ''
        if self.s_box.active:
            action = self.s_box.decision(self.snap_shot)
            if action =='restart':
                self.s_box.__init__(box_with = self.box_with,tofirm_time = self.tofirm_time,tofirm_profit = self.tofirm_profit)
                print('restart:df size-',self.s_box.data_df.shape[0])
            elif action == 'sale':
                self.s_box.__init__(box_with = self.box_with,tofirm_time = self.tofirm_time,tofirm_profit = self.tofirm_profit)
                sellprice = self.snap_shot.Last - 0.01
                sellvol = self.trade_stat.hold_volume
                if self.trade_stat.moneyplay :
                    self.sell_IB(sellprice,sellvol)
                    trdcontrol.moneyInplay += sellprice * sellvol
                else:
                    self.sell_simu(sellprice,sellvol)
            else:
                pass
#         print('average price:',self.trade_stat.avg_price)
        if self.snap_shot.Last >  self.trade_stat.avg_price*1.001 and self.s_box.active == False and action !='sale':
            print('break out ',self.snap_shot.datetime_t)
            self.s_box.break_out(self.snap_shot, self.trade_stat.avg_price)
#             debug['breaktimes']+= 1
        
        Marketclose = datetime.datetime(self.snap_shot.datetime_t.year, self.snap_shot.datetime_t.month, self.snap_shot.datetime_t.day, 15, 55, 0)
        if (self.snap_shot.Last >  self.trade_stat.avg_price*1.001) and (self.snap_shot.datetime_t > Marketclose):
            sellprice = self.snap_shot.Last - 0.01
            sellvol = self.trade_stat.hold_volume
            if self.trade_stat.moneyplay :
                self.sell_IB(sellprice,sellvol)
                trdcontrol.moneyInplay += sellprice * sellvol
            else:
                self.sell_simu(sellprice,sellvol)
            print('Market close sale')
            
    def simple_sell(self):
        
        cur_profit = self.snap_shot.Last/self.trade_stat.avg_price - 1.0
        
        '''
        calculate the profit threshold
        before salvation start time the profit target is start_threshold
        between the salvation start and salvation end time the profit target will reduce linearly to the end_threshold
        after salvation end time the profit target will stay on the end threshold
        '''
        now_time = self.snap_shot.datetime_t
        salvation_end = datetime.datetime(now_time.year
                                        , now_time.month
                                        , now_time.day
                                        , 15, 0, 0) 
        salvation_start = datetime.datetime(now_time.year
                                        , now_time.month
                                        , now_time.day
                                        , 14, 0, 0) 
        salvation_range = (salvation_end - salvation_start).total_seconds()
        
 
        
        if self.snap_shot.Low < self.trade_stat.buytime_min: #when the min at buy time is broke through then just keep the money.
            start_threshold = 2.0/trdcontrol.trade_size_low
        else:
            start_threshold = 0.002
        end_threshold = -0.002
        
        if now_time < salvation_start:
            profit_threshold = start_threshold
        elif now_time < salvation_end:
            salvation_life = (now_time - salvation_start).total_seconds() 
#             salvation_life = (the_time - salvation_start).total_seconds()       
            profit_threshold = start_threshold + (end_threshold-start_threshold)/salvation_range * salvation_life
        else:
            profit_threshold = end_threshold
        
        
        sale_action = False
        if cur_profit >=profit_threshold:
            sale_action =True
            
#         if self.snap_shot.datetime_t> midsell and cur_profit >0.0018:
#             sale_action = True
#         
#         if self.snap_shot.datetime_t> marketclose and cur_profit >-0.0005:
#             sale_action = True
        #When above: follow trend and trace 1/3 profit from top post buy capped at 1%
        
        #sale at close:   
        
        if sale_action:
            sellprice = self.snap_shot.Last - 0.01
            sellvol = self.trade_stat.hold_volume
            if self.trade_stat.moneyplay :
                self.sell_IB(sellprice,sellvol)
                trdcontrol.moneyInplay += sellprice * sellvol
            else:
                self.sell_simu(sellprice,sellvol)  
                
    def track_sell(self):
        #when bellow : don't sell until -2%
        marketclose = datetime.datetime(self.snap_shot.datetime_t.year, self.snap_shot.datetime_t.month, self.snap_shot.datetime_t.day, 15, 58, 0)
        Am10 = datetime.datetime(self.snap_shot.datetime_t.year, self.snap_shot.datetime_t.month, self.snap_shot.datetime_t.day, 10, 0, 0)
        high_profit = self.trade_stat.postbuy_high/self.trade_stat.avg_price -1.0
        cur_profit = self.snap_shot.Last/self.trade_stat.avg_price - 1.0
        
        
        sale_action = False
#         if self.snap_shot.datetime_t >Am10 and cur_profit < -0.02 and self.d_engine.current_trend.main_trend<0:
#             sale_action = True
        if high_profit > 0.005 and cur_profit < min(high_profit -0.005, high_profit *0.66 ):
            sale_action = True
        if self.snap_shot.datetime_t> marketclose:
            sale_action = True
        #When above: follow trend and trace 1/3 profit from top post buy capped at 1%
        
        #sale at close:   
        
        if sale_action:
            sellprice = self.snap_shot.Last - 0.01
            sellvol = self.trade_stat.hold_volume
            if self.trade_stat.moneyplay :
                self.sell_IB(sellprice,sellvol)
                trdcontrol.moneyInplay += sellprice * sellvol
            else:
                self.sell_simu(sellprice,sellvol)            
                
    def sell_IB(self,sellprice,sellvol):
        
        sellprice = myfloor(sellprice,2,self.trade_stat.mintrade)
        
        
        self.trade_stat.place_500(sellprice, sellvol, 'SELL')
        
        self.trade_stat.trade_profit += (sellprice - self.trade_stat.avg_price)*sellvol
        self.trade_stat.trade_profit_pct += (sellprice - self.trade_stat.avg_price)/self.trade_stat.avg_price
        trdcontrol.tot_profit += (sellprice - self.trade_stat.avg_price)*sellvol
        
        self.trade_stat.buysellstatus = 'ToBuy'
        print('simu sell '+str(sellvol)+' '+self.trade_stat.symbol+' at price:'+str(sellprice)+'profit:'+str(trdcontrol.tot_profit)+ 'time:'+str(self.snap_shot.datetime_t))
        log_act('Money sell '+str(sellvol)+' '+self.trade_stat.symbol+' at price:'+str(sellprice)+ 'time:'+str(self.snap_shot.datetime_t))
        
    def sell_simu(self,sellprice,sellvol):
        self.trade_stat.hold_volume = 0
        self.trade_stat.buysellstatus = 'ToBuy'
        self.trade_stat.last_buy = 0
        self.trade_stat.tot_cost = 0
        self.trade_stat.trade_profit += (sellprice - self.trade_stat.avg_price)*sellvol
        self.trade_stat.trade_profit_pct += (sellprice - self.trade_stat.avg_price)/self.trade_stat.avg_price
        trdcontrol.tot_profit += (sellprice - self.trade_stat.avg_price)*sellvol
        print('simu sell '+str(sellvol)+' '+self.trade_stat.symbol+' at price:'+str(sellprice)+'profit:'+str(trdcontrol.tot_profit)+ 'time:'+str(self.snap_shot.datetime_t))
        log_act('simu sell '+str(sellvol)+' '+self.trade_stat.symbol+' at price:'+str(sellprice)+ 'time:'+str(self.snap_shot.datetime_t))  
#         self.debug = False  
#         debug['soldvol'] += sellvol
#         debug['soldcost'] += sellvol * sellprice 
        
    def first_trend_buy(self,action):
        #get the first/last row of
        row = self.d_engine.modeldataB.iloc[-1]
#         time_t = self.d_engine.model_trend_output.index[-1]
#         row = ME.V_data(time_t, row_t)
        score = trdcontrol.para_prediction(row,0)
        print('score:',score)
        if score >0.72:
            buyprice = row['BuyPrice']
            #
            if action == 1:
                
                fund = min(trdcontrol.trade_size_high,max(trdcontrol.trade_size_low,trdcontrol.moneyInplay * self.trade_stat.kelly_pct*1.5))
    #             buyvol = int(self.trade_stat.basefund/buyprice)
                buyvol = int(fund/buyprice)
                self.trade_stat.buytime_min = self.snap_shot.Low
                
            elif action == 2:
                buyvol = self.trade_stat.hold_volume
                
            #set track of post buy
            self.trade_stat.postbuy_high = row['BuyPrice']
            self.trade_stat.postbuy_low = row['BuyPrice']
            self.trade_stat.last_buy = buyprice
            
            #double buy when average price is 4 % lower than the avarage price
            if buyprice /self.trade_stat.avg_price <0.96: 
                buyvol *= 2
            
            if self.trade_stat.moneyplay :
                if self.trade_stat.buysellstatus =='ToBuy' and trdcontrol.moneyInplay > trdcontrol.margin_buget :
                    self.buy_IB(buyprice,buyvol)
                    trdcontrol.moneyInplay -= buyprice * buyvol
                elif self.trade_stat.hold_volume > 0: #second by
                    
                    self.buy_IB(buyprice,buyvol)
                    trdcontrol.moneyInplay -= buyprice * buyvol
                
                    
            else:
#                 self.trade_stat.moneyplay = False
                self.buy_simu(buyprice,buyvol)
                
    def post_first_buy(self):
        #get the first/last row of
        row = self.d_engine.modeldataB.iloc[-1]
        time_t = self.d_engine.modeldataB.index[-1]

        score = trdcontrol.para_prediction(row,0)
        print('score:',score, '  ',time_t)
        if score > 0.73 :
            buyprice = row['BuyPrice'] + 0.01
            buyvol = int(self.trade_stat.basefund/buyprice)
            
            #set start of post buy
            self.trade_stat.postbuy_high = row['break_value']
            self.trade_stat.postbuy_low = row['break_value']
            self.trade_stat.last_buy = row['break_value']
            if self.trade_stat.moneyplay and trdcontrol.moneyInplay > 1000:
                self.buy_IB(buyprice,buyvol)
                #control the total money in play
                trdcontrol.moneyInplay -= buyprice * buyvol
            else:
                self.trade_stat.moneyplay = False
                self.buy_simu(buyprice,buyvol)
    
            
    def buy_IB(self,buyprice,buyvol):
        
        buyprice = myfloor(buyprice,2,self.trade_stat.mintrade)
        print('buy in buy_IB',buyprice)
        self.trade_stat.place_500(buyprice, buyvol, 'BUY')
        self.trade_stat.buysellstatus = 'BuySubmitted'
        log_act('Money buy '+str(buyvol)+' '+self.trade_stat.symbol+' at price:'+str(buyprice)+ 'time:'+str(self.snap_shot.datetime_t))
        
    def buy_simu(self,buyprice,buyvol):
        self.trade_stat.hold_volume += buyvol
        self.trade_stat.tot_cost  += buyvol * buyprice
        self.trade_stat.avg_price = self.trade_stat.tot_cost/self.trade_stat.hold_volume
#         print('average price:',self.trade_stat.avg_price,'tot cost:',self.trade_stat.tot_cost,'tot volume:',self.trade_stat.hold_volume)
        self.trade_stat.buysellstatus = 'ToSell'
        print('simu buy'+str(buyvol)+self.trade_stat.symbol+' at price:'+str(buyprice)+ 'time:'+str(self.snap_shot.datetime_t))
#         debug['buyvol'] += buyvol
#         debug['buycost'] +=buyvol * buyprice
        log_act('simu buy '+str(buyvol)+' '+self.trade_stat.symbol+' at price:'+str(buyprice)+ 'time:'+str(self.snap_shot.datetime_t))
  
        #decision point judgement
    def null_t(self):
        return 0        


if __name__ == '__main__':
    Datestr = datetime.date.today().strftime("%Y%m%d")

    global trdcontrol
    trdcontrol = TradeControl(budget = 500.0) #set a super high limit for all simulated case.
    
    simulate = False
        
    
    if simulate :
        pass
    else:
        #load in model
        now_time = datetime.datetime.now()
        check_time = datetime.datetime(now_time.year
                                        , now_time.month
                                        , now_time.day
                                        , 15
                                        , 58
                                        , 30)
        open_time = datetime.datetime(now_time.year
                                        , now_time.month
                                        , now_time.day
                                        , 9
                                        , 30
                                        , 5)

        pick_time = datetime.datetime(now_time.year
                                        , now_time.month
                                        , now_time.day
                                        , 9
                                        , 40
                                        , 0)
        end_day = datetime.date(year = now_time.year,month = now_time.month,day = now_time.day)
        start_day = end_day+datetime.timedelta(days = -7)
        end_day_s = end_day.isoformat()
        start_day_s = start_day.isoformat()
        
        out_df1 = pdr.get_data_yahoo(tickers = ['^DJI']
                                      ,start = start_day_s
                                      ,end = end_day_s
                                      ,as_panel = False)   
        DJI_prior = out_df1.Close[-1]
        print('Prior DJI closed at:',DJI_prior)
        
        basePath = '/home/wang/Documents/Production/DaliyModel/'
        model_path = basePath+'model/'
        data_path = basePath + 'data/'
        data_out = basePath + 'output/'
        rpt_out = basePath + 'rptoutnew/'
        file_t = model_path+'ScoreMax1.pickled'
        
        trdcontrol.load_model(file_t)
        
        #load in symbol list
#         input_date = sys.argv[1]
#         trdcontrol.symbol_list = pd.DataFrame.from_csv(data_path+input_date+'today.csv')
        tmp = pd.read_csv(data_path+'20180716today.csv',index_col = 0)
        print('Picked: ',tmp.moneyplay.sum())
        trdcontrol.symbol_list = tmp[~tmp.index.duplicated( keep='first')]
        
        trdcontrol.init_smartlist()
        
#         trdcontrol.init_trade_size()
        
#         trdcontrol.smartlist['AAPL'].trade_stat.moneyplay
       
        #sleep to time 
#         sleepto(9,28,30)
        
        
        #connect to IB
        trdcontrol.makeconnection()
        #get account value
        trdcontrol.get_account_value()
        
        #request position
        trdcontrol.request_position()
        #request market data
        trdcontrol.request_market_data()
        #sleep to market end
        sleep(10)
        
        
        
        print('start looping')
        #sleepto(9,30,0)
        DJI_v = []
        DJI_t = []
        
        moneyplay_picked = False
        sec_count = 0
        while datetime.datetime.now()<check_time :
            t = time.time()
            sec_count += 1
#             print(sec_count)
            for symbol_t,smart_ib_i in trdcontrol.smartlist.items():
                #accumulate the DJI value
                old_price = trdcontrol.DJI_dic.get(symbol_t, -1)
                if old_price != -1: #if return -1 then this symbol is not in the DJI 30
                    if trdcontrol.smartlist[symbol_t].snap_shot.Last !=old_price:
                        trdcontrol.DJI_cal += (trdcontrol.smartlist[symbol_t].snap_shot.Last -old_price)*trdcontrol.DJI_multi
                        trdcontrol.DJI_dic[symbol_t] = trdcontrol.smartlist[symbol_t].snap_shot.Last
                #only start to trade when level 2 pick is done
                if moneyplay_picked:
                    smart_ib_i.decision()
                    
            DJI_v.append(trdcontrol.DJI_cal)
            DJI_t.append(datetime.datetime.now())
            #only do once at the time pick time 9:40 in the morning        
            if datetime.datetime.now()>pick_time and moneyplay_picked == False:
                moneyplay_picked = True
                    #get prior day index
#                 DJI_v = [2345,3456]
#                 DJI_t = [1,2]
                DJI_df_t = pd.DataFrame({'DJI':DJI_v,'Time':DJI_t})
                DJI_df_t = DJI_df_t[DJI_df_t.Time>open_time]
                
                if DJI_df_t.shape[0]>0:
                    print('DJI_df_t.shape:',DJI_df_t.shape)
                    
                    DJI_list = list(DJI_df_t.DJI)
                    DJI_open = DJI_list[0]
                    DJI_Nmin = DJI_list[-1]#DJI[DJI_df_t.shape[0]-1]
                    DJI_oj = DJI_open/DJI_prior - 1.0
                    DJI_trd = DJI_Nmin/DJI_open -1.0 
                    print('market open jump:',DJI_oj,' Market trend :',DJI_trd)
                else:
                    print('DJI data not collected')
                    DJI_oj = -0.5
                    DJI_trd = -0.5
                    
                inplay = 0
                final_pick = []
                for symbol_t,smart_ib_t in trdcontrol.smartlist.items():
                    if smart_ib_t.trade_stat.moneyplay:
                        symbol_dt = smart_ib_t.d_engine.rawdata.copy()
                        symbol_dt = symbol_dt[symbol_dt.index>open_time]
                        if symbol_dt.shape[0]>0:
                            last_list = list(symbol_dt.Last)
                            symbol_open = last_list[0]
                            symbol_Nmin = last_list[-1]
                            Close_list = list(symbol_dt.Close)
                            symbol_prior = Close_list[-1]
                            symbol_oj = symbol_open/symbol_prior - 1.0
                            symbol_trd = symbol_Nmin/symbol_open -1.0
                            symbol2indextrd = symbol_trd - DJI_trd
                            print(symbol_t, 'stk open jump:',symbol_oj,' stk trend :',symbol_trd)
                            smart_ib_t.trade_stat.moneyplay = pick_rule(DJI_oj,symbol_oj,DJI_trd,symbol_trd)
                        else:
                            print(symbol_t,'Data not collected')
                            smart_ib_t.trade_stat.moneyplay = False
                            
                        if smart_ib_t.trade_stat.moneyplay:
                            print(symbol_t, ' Pikced')
                            inplay +=1
                            final_pick.append(symbol_t)
                            
                print('total stock picked', inplay)
                trdcontrol.init_trade_size(inplay)
#                 DJI_oj= 0.1234
#                 DJI_trd = -0.345
#                 str(DJI_oj)
#                 str(DJI_trd)  
                
                ini_log([str(DJI_oj),str(DJI_trd)]+final_pick)        
                    
                    #calculate the 
                        
            for key,val in trdcontrol.DJI_dic.items():
#                 print(key, val)
                if val ==0:
                    print(key,' not get price')  
#                     tickID = trdcontrol.smartlist[symbol_t].trade_stat.tick_id
#                     contract = trdcontrol.smartlist[symbol_t].trade_stat.contract
#                     trdcontrol.con.reqMktData(int(tickID), contract, '', False)    
      
            
                
            #need to save the DJI_Cal data

            elapsed_time = time.time() - t
            
#             print('loop time:',elapsed_time, ' DJI:', trdcontrol.DJI_cal)
            sleep(1)
            
        sleepto(15,59,0)
        
        #use half of the day's profit to pay for loss
#         trdcontrol.close_sale()
        trdcontrol.close_sale()
        trdcontrol.profit_report(rpt_out)
        
        sleepto(16,0,0)
        
        
        #close connection
        trdcontrol.con.disconnect()
        
        #save data
        trdcontrol.save_data(data_out)
        
        DJI_df = pd.DataFrame({'DJI':DJI_v,'Time':DJI_t})
        DJI_df.to_csv(data_out+'DJI'+Datestr+'.csv')
        
        


        
        
        
