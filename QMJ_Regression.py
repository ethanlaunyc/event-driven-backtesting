# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 14:41:34 2018

@author: eliu
"""
### Two step regression

# =============================================================================
# #Do cross-sectional regression of QMJ on size y = ax + b, size = a*QMJ + b and
#  take the residual, which means exclude QMJ effect from Size
# =============================================================================
import pandas as pd
import numpy as np
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns
import math
import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf
con = pyodbc.connect('DRIVER={SQL Server};'
                         'SERVER=liangpi.sql.intra.gsacapital.com;'
                         'DATABASE=LIANGPI;'
                         'Trusted_Connection=yes;')
cursor = con.cursor()

sql1 = "select * from QMJ2 order by dates,dscode  "
df1 = pd.read_sql(sql1,con)
##generate list of distinct days
days = df1['dates'].unique()
l = list(days)
cursor.commit()
cursor.close()

names = list(df1)
names.append('residual')
## generate names&dataframes for the final long/short dataframe

df = pd.DataFrame(columns = names)

days_test = l[0:20]
a = df1[0:1000]

#x = np.array(a.QMJ)
#X = sm.add_constant(x)
#y = np.array(a.zscore_NLMktCap)
#model = sm.OLS(y,X,missing = 'drop').fit()
#residual = a.zscore_NLMktCap - model.predict(X)
#b = model.predict(X)
#print(model.summary())


#x1 = np.atleast_2d(a.QMJ).T
#X1 = sm.add_constant(x1)
#y1 = np.atleast_2d(a.zscore_NLMktCap).T
#model1 = sm.OLS(y1,X1,missing = 'drop').fit()
#residual2 = a.zscore_NLMktCap - model.predict(X1)


# =============================================================================
# loop through each day for cross-sectional regression
# =============================================================================
## Step1: Quality(y) ~ Size(x) + residual(QMJ)
for day in days:
    daily_df = df1[df1['dates'] == day]
    x = np.array(daily_df.zscore_NLMktCap)
    X = sm.add_constant(x)
    y = np.array(daily_df.zscore_quality)
    model = sm.OLS(y,X).fit()
    daily_df['residual'] = daily_df['zscore_quality'] - model.predict(X)
    df = df.append(daily_df, ignore_index = True)
    
    print('Finished', day)


names.append('new_residual')
new_df = pd.DataFrame(columns = names)


## Step2    
for day in days:
    daily_df = df[df['dates'] == day]
    x = np.array(daily_df.residual)
    X = sm.add_constant(x)
    y = np.array(daily_df.zscore_NLMktCap)
    model = sm.OLS(y,X).fit()
    daily_df['new_residual'] = daily_df['zscore_NLMktCap'] - model.predict(X)
    new_df = new_df.append(daily_df, ignore_index = True)
    
    print('Finished', day)    
    
    
    
    
    
    
### Calculate the zscore of residual
for day in days:
    new_df['zscore_residual'] = (new_df.new_residual - new_df.new_residual.mean())/new_df.new_residual.std()
    print('Finished', day)

## check if standardized
a = new_df[new_df['dates']=='2005-01-04']
b = a.sort_values(by = ['zscore_residual'])
    
# =============================================================================
# Re-run the test by using z_residual    
# =============================================================================
daily_short = pd.DataFrame(columns = names)
daily_long = pd.DataFrame(columns = names)

daily_short2 = pd.DataFrame(columns = names)
daily_long2 = pd.DataFrame(columns = names)

for day in days:
    
    ######################################################
    ##Use absolute return
    ######################################################      
    daily_df = new_df[new_df['dates'] == day]
    ###Short side loop
    daily_ShortSide = daily_df[daily_df['zscore_residual'] <= 0] 
    n = len(daily_ShortSide)
    daily_ShortSide['port_position'] = daily_ShortSide['zscore_residual']/sum(daily_ShortSide['zscore_residual'])*(-1)
    daily_ShortSide['port_return'] = daily_ShortSide['lead_2day_return']*daily_ShortSide['port_position']
    daily_short = daily_short.append(daily_ShortSide, ignore_index = True)
    groupeddaily_short = daily_short.groupby(['dates'])['port_return'].sum()
        
    ###long side loop
    daily_LongSide = daily_df[daily_df['zscore_residual'] > 0]
    n = len(daily_LongSide)
    daily_LongSide['port_position'] = daily_LongSide['zscore_residual']/sum(daily_LongSide['zscore_residual'])
    daily_LongSide['port_return'] = daily_LongSide['lead_2day_return']* daily_LongSide['port_position']
    daily_long = daily_long.append(daily_LongSide, ignore_index = True)
    groupeddaily_long = daily_long.groupby(['dates'])['port_return'].sum()
        
    
    ######################################################
    ##Use Residual return:FACTORS_4_ind
    ######################################################    
    daily_df2 = new_df[new_df['dates'] == day]
    ###Short side loop
    daily_ShortSide2 = daily_df2[daily_df2['zscore_residual'] <= 0] 
    n2 = len(daily_ShortSide2)
    daily_ShortSide2['port_position'] = daily_ShortSide2['zscore_residual']/sum(daily_ShortSide2['zscore_residual'])*(-1)
    daily_ShortSide2['port_return'] = daily_ShortSide2['lead_2day_residual_return']*daily_ShortSide2['port_position']
    daily_short2 = daily_short2.append(daily_ShortSide2, ignore_index = True)
    groupeddaily_short2 = daily_short2.groupby(['dates'])['port_return'].sum()
        
    ###long side loop
    daily_LongSide2 = daily_df2[daily_df2['zscore_residual'] > 0]
    n2 = len(daily_LongSide2)
    daily_LongSide2['port_position'] = daily_LongSide2['zscore_residual']/sum(daily_LongSide2['zscore_residual'])
    daily_LongSide2['port_return'] = daily_LongSide2['lead_2day_residual_return']* daily_LongSide2['port_position']
    daily_long2 = daily_long2.append(daily_LongSide2, ignore_index = True)
    groupeddaily_long2 = daily_long2.groupby(['dates'])['port_return'].sum()
    
    print('Finished',day)    
    
    
# =============================================================================
# calculate portfolio return and plot --daily
# =============================================================================
##Calculate cumulated sum return and plot
#groupeddaily_long = groupeddaily_long[groupeddaily_long.index >= '2010-01-12']*(-1)
#groupeddaily_short = groupeddaily_short[groupeddaily_short.index >= '2010-01-12']  *(-1)
#groupeddaily_long2 = groupeddaily_long2[groupeddaily_long2.index >= '2010-01-12']*(-1)
#groupeddaily_short2 = groupeddaily_short2[groupeddaily_short2.index >= '2010-01-12'] *(-1)
#    
    
    
longSide_daily = groupeddaily_long.cumsum() * -1 +1
shortSide_daily = groupeddaily_short.cumsum() * -1+1
portfolio_daily = longSide_daily + shortSide_daily-1

dic = {'LongSide_daily':longSide_daily,'ShortSide_daily':shortSide_daily,
       'Portfolio_daily':portfolio_daily,'dates':longSide_daily.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
daily = pd.DataFrame(data = dic)

daily.dates = pd.to_datetime(daily['dates'],format = '%Y-%m-%d' )
daily.set_index(['dates'],inplace = True)
daily.plot(grid = True, title = 'Quality Factor: Use Absolute Return')
#daily.Portfolio_daily.plot(grid = True, title = 'score weighted TRMI_NewsSocial_sentiment factor long/short daily')



longSide_daily2 = groupeddaily_long2.cumsum() * -1+1
shortSide_daily2 = groupeddaily_short2.cumsum() * -1 +1
portfolio_daily2 = longSide_daily2 + shortSide_daily2-1

dic = {'LongSide_daily':longSide_daily2,'ShortSide_daily':shortSide_daily2,
       'Portfolio_daily':portfolio_daily2,'dates':longSide_daily2.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
daily2 = pd.DataFrame(data = dic)

daily2.dates = pd.to_datetime(daily2['dates'],format = '%Y-%m-%d' )
daily2.set_index(['dates'],inplace = True)
daily2.plot(grid = True, title = 'QMJ Use Residual Return, Regress on size')
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
#mdd = MaxDrawdown(portfolio_daily)


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
 title = 'Quality factor(use absolute(MD2) return strategy = FACTORS_4_ind) AnnSharpe: 1.651, AnnVol: 0.038, AnnReturn: 0.063,\
 MDD: 0.087,delay:1, bks:1*1, stk:644-653')

daily2.Portfolio_daily.plot(grid = True, 
 title = 'Regression, Step 2: Size(y) ~ QMJ(x) + residual2,\
 AnnSharpe: 0.85, AnnVol: 0.061, AnnReturn: 0.052,\
 MDD: 0.106,delay:1, bks:1*1, stk:680-618')    
    
    
    
    
# =============================================================================
#     regenerate the position  
# =============================================================================
    
    
    
##get 5 factors and join table    
cursor = con.cursor()

sql2 = "select * from QMJ_Position_Regression order by dates,dscode  "
df2 = pd.read_sql(sql2,con)    

##merge 5 factors and position
df3 = pd.concat([daily_long2,daily_short2])
  
df4 = pd.merge(df3,df2, on = ['dates','dscode'],how = 'left')    

##Do multi-linear regression and regenerate the zscore as new position
##Industry column is categorical variable

names2 = list(df4)
names2.append('result')
names2.append('new_position')
## generate names&dataframes for the final long/short dataframe

df_regression = pd.DataFrame(columns = names)

df5 = df4[['dates','dscode','zscore_quality_y','Predicted_Beta','Medium_term_momentum',\
              'value','size','LPIndustry']]
names2 = list(df5)
names2.append('result')
#names2.append('new_position')
## generate names&dataframes for the final long/short dataframe

df6 = pd.DataFrame(columns = names)


#Do the regression
for day in days:
    daily_df = df5[df5['dates'] == day ].dropna()
    res = smf.ols(formula='zscore_quality_y ~ Predicted_Beta + Medium_term_momentum\
              + value + size + C(LPIndustry)', data=daily_df).fit()
    daily_df['result'] = res.predict() 
    df6 = df6.append(daily_df, ignore_index = True)
    print('Finished', day)

#standardize the result column and get new position
for day in days:
    df6['new_result'] = df6['result'] - df6['zscore_quality_y']
#    df6['new_position'] = (df6['new_result'] - df6['new_result'].mean())/df6['new_result'].std()
    print('Finished', day)
    
for day in days:
    df6['new_position'] = (df6['new_result'] - df6['new_result'].mean())/df6['new_result'].std()
    print('Finished', day)

aa = df6[0:1000]
bb = aa.sort_values(by = ['new_position'])

df7 = df6[['dates','dscode','new_position']]

##join new position and return table



