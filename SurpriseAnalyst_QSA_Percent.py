
# =============================================================================
# IHS_SurpriseAnalyst_QSA_Percent factor
# equal weighted and alpha weighted
# =============================================================================

import pandas as pd
import numpy as np
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns
import math
import numpy as np
con = pyodbc.connect('DRIVER={SQL Server};'
                         'SERVER=liangpi.sql.intra.gsacapital.com;'
                         'DATABASE=LIANGPI;'
                         'Trusted_Connection=yes;')
cursor = con.cursor()

## get the rank_returns table from sql server
sql1 = "select * from IHS_SurpriseAnalyst_QSA_Percent order by dates,dscode  "
df1 = pd.read_sql(sql1,con)
##generate list of distinct days
days = df1['dates'].unique()
l = list(days)
cursor.commit()
cursor.close()

## generate names&dataframes for the final long/short dataframe
daily_names = ['dates','dscode','lead_1day_position','lead_1day_return',
               'a','b','port_position','port_return']
daily_short = pd.DataFrame(columns = daily_names)
daily_long = pd.DataFrame(columns = daily_names)

daily_short2 = pd.DataFrame(columns = daily_names)
daily_long2 = pd.DataFrame(columns = daily_names)
days_test = l[0:20]
a = df1[0:1000]

for day in days:
    ######################################################
    ##CG factor: equal weighted
    ######################################################
    daily_df = df1[df1['dates'] == day]
    ###Short side loop
    daily_ShortSide = daily_df[daily_df['QSA_Percent'] <= daily_df['a']] 
    n = len(daily_ShortSide)
    daily_ShortSide['port_position'] = 1/n*(-1)
    daily_ShortSide['port_return'] = daily_ShortSide['lead_2day_return']*daily_ShortSide['port_position']
    daily_short = daily_short.append(daily_ShortSide, ignore_index = True)
    groupeddaily_short = daily_short.groupby(['dates'])['port_return'].sum()
        
    ###long side loop
    daily_LongSide = daily_df[daily_df['QSA_Percent'] > daily_df['b']]
    n = len(daily_LongSide)
    daily_LongSide['port_position'] = 1/n
    daily_LongSide['port_return'] = daily_LongSide['lead_2day_return']* daily_LongSide['port_position']
    daily_long = daily_long.append(daily_LongSide, ignore_index = True)
    groupeddaily_long = daily_long.groupby(['dates'])['port_return'].sum()

    
    ######################################################
    ##CG factor: alpha weighted
    ######################################################    
    daily_df2 = df1[df1['dates'] == day]
    ###Short side loop
    daily_ShortSide2 = daily_df2[daily_df2['zscore_QSA_Percent'] <= 0] 
    n2 = len(daily_ShortSide2)
    daily_ShortSide2['port_position'] = daily_ShortSide2['zscore_QSA_Percent']/sum(daily_ShortSide2['zscore_QSA_Percent'])*(-1)
    daily_ShortSide2['port_return'] = daily_ShortSide2['lead_2day_return']*daily_ShortSide2['port_position']
    daily_short2 = daily_short2.append(daily_ShortSide2, ignore_index = True)
    groupeddaily_short2 = daily_short2.groupby(['dates'])['port_return'].sum()
        
    ###long side loop
    daily_LongSide2 = daily_df2[daily_df2['zscore_QSA_Percent'] > 0]
    n2 = len(daily_LongSide2)
    daily_LongSide2['port_position'] = daily_LongSide2['zscore_QSA_Percent']/sum(daily_LongSide2['zscore_QSA_Percent'])
    daily_LongSide2['port_return'] = daily_LongSide2['lead_2day_return']* daily_LongSide2['port_position']
    daily_long2 = daily_long2.append(daily_LongSide2, ignore_index = True)
    groupeddaily_long2 = daily_long2.groupby(['dates'])['port_return'].sum()
    print('Finished',day)    
    
    
# =============================================================================
# calculate portfolio return and plot --daily
# =============================================================================
##Calculate cumulated sum return and plot
#groupeddaily_long = groupeddaily_long[groupeddaily_long.index >= '2010-01-12']*-1
#groupeddaily_short = groupeddaily_short[groupeddaily_short.index >= '2010-01-12']  *-1
#groupeddaily_long2 = groupeddaily_long2[groupeddaily_long2.index >= '2010-01-12']*-1
#groupeddaily_short2 = groupeddaily_short2[groupeddaily_short2.index >= '2010-01-12']  *-1
#    
#    
    
longSide_daily = groupeddaily_long.cumsum()+1
shortSide_daily = groupeddaily_short.cumsum()+1
portfolio_daily = longSide_daily + shortSide_daily-1

dic = {'LongSide_daily':longSide_daily,'ShortSide_daily':shortSide_daily,
       'Portfolio_daily':portfolio_daily,'dates':longSide_daily.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
daily = pd.DataFrame(data = dic)

daily.dates = pd.to_datetime(daily['dates'],format = '%Y-%m-%d' )
daily.set_index(['dates'],inplace = True)
daily.plot(grid = True, title = 'score weighted TRMI_NewsSocial_sentiment factor')
##daily.Portfolio_daily.plot(grid = True, title = 'score weighted TRMI_NewsSocial_sentiment factor long/short daily')



longSide_daily2 = groupeddaily_long2.cumsum()+1
shortSide_daily2 = groupeddaily_short2.cumsum()+1
portfolio_daily2 = longSide_daily2 + shortSide_daily2-1

dic = {'LongSide_daily':longSide_daily2,'ShortSide_daily':shortSide_daily2,
       'Portfolio_daily':portfolio_daily2,'dates':longSide_daily2.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
daily2 = pd.DataFrame(data = dic)

daily2.dates = pd.to_datetime(daily2['dates'],format = '%Y-%m-%d' )
daily2.set_index(['dates'],inplace = True)
daily2.plot(grid = True, title = 'score weighted TRMI_NewsSocial_emotionVSfact factor,buzz>20')
##daily2.Portfolio_daily2.plot(grid = True, title = 'score weighted TRMI_NewsSocial_emotionVSfact factor long/short daily')


# =============================================================================
# calculate portfolio performance
# =============================================================================

#####Part 1
#Turnover = pd.read_csv('turnover_buzz6080.csv')
port_return = groupeddaily_long + groupeddaily_short
Ann_vol = port_return.std() * math.sqrt(252)
Ann_Sharpe = port_return.mean()  / port_return.std()  * math.sqrt(252)
Ann_return = port_return.mean()*252
#AverageTurnover = Turnover.news_social_sentiment.mean()
a = df1[0:5]
##net value of portfolio(first day equals to 1)

def MaxDrawdown(return_list):

    i = np.argmax((np.maximum.accumulate(return_list) - return_list) / np.maximum.accumulate(return_list))  # 结束位置
    if i == 0:
        return 0
    j = np.argmax(return_list[:i])  
    return (return_list[j] - return_list[i]) / (return_list[j])
mdd = MaxDrawdown(portfolio_daily)


#####Part 2
port_return2 = groupeddaily_long2 + groupeddaily_short2
Ann_vol2 = port_return2.std() * math.sqrt(252)
Ann_Sharpe2 = port_return2.mean()  / port_return2.std()  * math.sqrt(252)
Ann_return2 = port_return2.mean()*252
#AverageTurnover2 = Turnover.news_emotion_buzz60.mean()

##net value of portfolio(first day equals to 1)
mdd2 = MaxDrawdown(portfolio_daily2)

###calculate stk(recent 1 year)
recent = daily[daily.index > '2017-01-01']
recent_long = daily_long[daily_long['dates']>'2017-01-01']
recent_short = daily_short[daily_short['dates']>'2017-01-01']
stk_long = len(recent_long)/len(recent)
stk_short = len(recent_short)/len(recent)

recent2 = daily2[daily2.index > '2017-01-01']
recent_long2 = daily_long2[daily_long2['dates']>'2017-01-01']
recent_short2 = daily_short2[daily_short2['dates']>'2017-01-01']
stk_long2 = len(recent_long2)/len(recent2)
stk_short2 = len(recent_short2)/len(recent2)

daily.Portfolio_daily.plot(grid = True, 
 title = 'Equal weighted  SurpirseAnalyst Percent(Probability of Surprise) factor long/short quintile daily \
 AnnSharpe: 0.405, AnnVol: 0.094, AnnReturn: 0.038,MDD: 0.173,delay:1,\
 bks:1*1, stk:375-403')

daily2.Portfolio_daily.plot(grid = True, 
 title = 'Alpha weighted SurpirseAnalyst Percent(Probability of Surprise) factor long/short daily \
 AnnSharpe: 0.393, AnnVol: 0.077, AnnReturn: 0.030,MDD: 0.147,delay:1,\
 bks:1*1, stk:977-968')    
    
    







   
# =============================================================================
# calculate portfolio return and plot --daily
# =============================================================================
##Calculate cumulated sum return and plot
groupeddaily_longd = groupeddaily_long*-1
groupeddaily_shortd = groupeddaily_short  *-1
groupeddaily_long2d = groupeddaily_long2*-1
groupeddaily_short2d = groupeddaily_short2  *-1
    
    
    
longSide_dailyd = groupeddaily_longd.cumsum()+1
shortSide_dailyd = groupeddaily_shortd.cumsum()+1
portfolio_dailyd = longSide_dailyd + shortSide_daily-1

dicd = {'LongSide_daily':longSide_dailyd,'ShortSide_daily':shortSide_dailyd,
       'Portfolio_daily':portfolio_dailyd,'dates':longSide_dailyd.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
dailyd = pd.DataFrame(data = dicd)

dailyd.dates = pd.to_datetime(dailyd['dates'],format = '%Y-%m-%d' )
dailyd.set_index(['dates'],inplace = True)
dailyd.plot(grid = True, title = 'score weighted TRMI_NewsSocial_sentiment factor')
##daily.Portfolio_daily.plot(grid = True, title = 'score weighted TRMI_NewsSocial_sentiment factor long/short daily')



longSide_daily2 = groupeddaily_long2.cumsum()+1
shortSide_daily2 = groupeddaily_short2.cumsum()+1
portfolio_daily2 = longSide_daily2 + shortSide_daily2-1

dic = {'LongSide_daily':longSide_daily2,'ShortSide_daily':shortSide_daily2,
       'Portfolio_daily':portfolio_daily2,'dates':longSide_daily2.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
daily2 = pd.DataFrame(data = dic)

daily2.dates = pd.to_datetime(daily2['dates'],format = '%Y-%m-%d' )
daily2.set_index(['dates'],inplace = True)
daily2.plot(grid = True, title = 'score weighted TRMI_NewsSocial_emotionVSfact factor,buzz>20')
##daily2.Portfolio_daily2.plot(grid = True, title = 'score weighted TRMI_NewsSocial_emotionVSfact factor long/short daily')


# =============================================================================
# calculate portfolio performance
# =============================================================================

#####Part 1
#Turnover = pd.read_csv('turnover_buzz6080.csv')
port_return = groupeddaily_long + groupeddaily_short
Ann_vol = port_return.std() * math.sqrt(252)
Ann_Sharpe = port_return.mean()  / port_return.std()  * math.sqrt(252)
Ann_return = port_return.mean()*252
#AverageTurnover = Turnover.news_social_sentiment.mean()
a = df1[0:5]
##net value of portfolio(first day equals to 1)

def MaxDrawdown(return_list):

    i = np.argmax((np.maximum.accumulate(return_list) - return_list) / np.maximum.accumulate(return_list))  # 结束位置
    if i == 0:
        return 0
    j = np.argmax(return_list[:i])  
    return (return_list[j] - return_list[i]) / (return_list[j])
mdd = MaxDrawdown(portfolio_daily)


#####Part 2
port_return2 = groupeddaily_long2 + groupeddaily_short2
Ann_vol2 = port_return2.std() * math.sqrt(252)
Ann_Sharpe2 = port_return2.mean()  / port_return2.std()  * math.sqrt(252)
Ann_return2 = port_return2.mean()*252
#AverageTurnover2 = Turnover.news_emotion_buzz60.mean()

##net value of portfolio(first day equals to 1)
mdd2 = MaxDrawdown(portfolio_daily2)

###calculate stk(recent 1 year)
recent = daily[daily.index > '2017-01-01']
recent_long = daily_long[daily_long['dates']>'2017-01-01']
recent_short = daily_short[daily_short['dates']>'2017-01-01']
stk_long = len(recent_long)/len(recent)
stk_short = len(recent_short)/len(recent)

recent2 = daily2[daily2.index > '2017-01-01']
recent_long2 = daily_long2[daily_long2['dates']>'2017-01-01']
recent_short2 = daily_short2[daily_short2['dates']>'2017-01-01']
stk_long2 = len(recent_long2)/len(recent2)
stk_short2 = len(recent_short2)/len(recent2)

daily.Portfolio_daily.plot(grid = True, 
 title = 'Equal weighted  SurpirseAnalyst Percent(Probability of Surprise) factor long/short quintile daily \
 AnnSharpe: 0.405, AnnVol: 0.054, AnnReturn: 0.021,MDD: 0.146,delay:1,\
 bks:1*1, stk:380-400')

daily2.Portfolio_daily.plot(grid = True, 
 title = 'Alpha weighted SurpirseAnalyst Percent(Probability of Surprise) factor long/short daily \
 AnnSharpe: 0.342, AnnVol: 0.043, AnnReturn: 0.015,MDD: 0.121,delay:1,\
 bks:1*1, stk:972-973')    
    
    
    
    