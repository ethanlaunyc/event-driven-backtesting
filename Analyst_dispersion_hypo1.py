# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 10:06:24 2018

@author: eliu
"""



# =============================================================================
# Backtest the Analyst Dispersion Hypothesis 1
# do the daily, weekly and monthly backtest seperately
# take the lead_2day_return, lead_1week_return and lead_1month_return as Return factor
# take the lead_1day_FC_CVFY1EPS(rank from 1-100) as position
# =============================================================================

import pandas as pd
import numpy as np
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
con = pyodbc.connect('DRIVER={SQL Server};'
                         'SERVER=liangpi.sql.intra.gsacapital.com;'
                         'DATABASE=LIANGPI;'
                         'Trusted_Connection=yes;')
cursor = con.cursor()


## get the rank_returns table from sql server
sql1 = "select a.* from [Analyst_Dispersion_hypo1_daily] a order by dates,lead_1day_position "
sql2 = "select a.* from [Analyst_Dispersion_hypo1_weekly] a order by dates,lead_1day_position "
sql3 = "select a.* from [Analyst_Dispersion_hypo1_monthly] a order by dates,lead_1day_position "

df1 = pd.read_sql(sql1,con)
df2 = pd.read_sql(sql2,con)
df3 = pd.read_sql(sql3,con)

## select unique trading days
days = df1['dates'].unique()
cursor.commit()
cursor.close()

## generate names&dataframes for the final long/short dataframe
daily_names = ['dates','dscode','lead_1day_position','lead_2day_return','a','b','port_return']
daily_short = pd.DataFrame(columns = daily_names)
daily_long = pd.DataFrame(columns = daily_names)

weekly_names = ['dates','dscode','lead_1day_position','lead_1week_return','a','b','port_return']
weekly_short = pd.DataFrame(columns = daily_names)
weekly_long = pd.DataFrame(columns = daily_names)

monthly_names = ['dates','dscode','lead_1day_position','lead_1month_return','a','b','port_return']
monthly_short = pd.DataFrame(columns = daily_names)
monthly_long = pd.DataFrame(columns = daily_names)

# make a test for several days
days_test = ['2004-01-02','2004-01-05','2004-01-06','2004-01-07','2004-01-08','2004-01-09',
             '2004-01-12','2004-01-13','2004-01-14','2004-01-15','2004-01-16','2004-01-20']





#plot_df = pd.DataFrame()
#plot_df = pd.DataFrame()
#plot_df = pd.DataFrame()

for day in days:
    ######################################################
    ##backtest daily data
    ######################################################
    daily_df = df1[df1['dates'] == day]
    ###Short side loop
    daily_ShortSide = daily_df[daily_df['lead_1day_position'] >= daily_df['b']] 
    n = len(daily_ShortSide)
    daily_ShortSide['port_return'] = daily_ShortSide['lead_2day_return']/n*(-1)
    daily_short = daily_short.append(daily_ShortSide, ignore_index = True)
    groupeddaily_short = daily_short.groupby(['dates'])['port_return'].sum()
        
    ###long side loop
    daily_LongSide = daily_df[daily_df['lead_1day_position'] <= daily_df['a']]
    n = len(daily_LongSide)
    daily_LongSide['port_return'] = daily_LongSide['lead_2day_return']/n
    daily_long = daily_long.append(daily_LongSide, ignore_index = True)
    groupeddaily_long = daily_long.groupby(['dates'])['port_return'].sum()
    

    ######################################################
    ##backtest weekly data
    ######################################################
    weekly_df = df2[df2['dates'] == day]
    ###Short side loop
    weekly_ShortSide = weekly_df[weekly_df['lead_1day_position'] >= weekly_df['b']] 
    n = len(weekly_ShortSide)
    weekly_ShortSide['port_return'] = weekly_ShortSide['lead_1week_return']/n*(-1)
    weekly_short = weekly_short.append(weekly_ShortSide, ignore_index = True)
    groupedweekly_short = weekly_short.groupby(['dates'])['port_return'].sum()
        
    ###long side loop
    weekly_LongSide = weekly_df[weekly_df['lead_1day_position'] <= daily_df['a']]
    n = len(weekly_LongSide)
    weekly_LongSide['port_return'] = weekly_LongSide['lead_1week_return']/n
    weekly_long = weekly_long.append(weekly_LongSide, ignore_index = True)
    groupedweekly_long = weekly_long.groupby(['dates'])['port_return'].sum()    
    
    
    
    ######################################################
    ##backtest monthly data
    ######################################################
    monthly_df = df3[df3['dates'] == day]
    ###Short side loop
    monthly_ShortSide = monthly_df[monthly_df['lead_1day_position'] >= monthly_df['b']] 
    n = len(monthly_ShortSide)
    monthly_ShortSide['port_return'] = monthly_ShortSide['lead_1month_return']/n*(-1)
    monthly_short = monthly_short.append(monthly_ShortSide, ignore_index = True)
    groupedmonthly_short = monthly_short.groupby(['dates'])['port_return'].sum()
        
    ###long side loop
    monthly_LongSide = monthly_df[monthly_df['lead_1day_position'] <= monthly_df['a']]
    n = len(monthly_LongSide)
    monthly_LongSide['port_return'] = monthly_LongSide['lead_1month_return']/n
    monthly_long = monthly_long.append(monthly_LongSide, ignore_index = True)
    groupedmonthly_long = monthly_long.groupby(['dates'])['port_return'].sum()    
    
    print('finished',day)


# =============================================================================
# calculate portfolio return and plot --daily
# =============================================================================
##Calculate cumulated sum return and plot
longSide_daily = groupeddaily_long.cumsum()
shortSide_daily = groupeddaily_short.cumsum()
portfolio_daily = longSide_daily + shortSide_daily

dic = {'LongSide_daily':longSide_daily,'ShortSide_daily':shortSide_daily,
       'Portfolio_daily':portfolio_daily,'dates':longSide_daily.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
daily = pd.DataFrame(data = dic)

daily.dates = pd.to_datetime(daily['dates'],format = '%Y-%m-%d' )
daily.set_index(['dates'],inplace = True)
daily.LongSide_daily.plot(grid = True, title = 'Analyst Dispersion Hypo2_daily')
daily.Portfolio_daily.plot(grid = True, title = 'Analyst Dispersion Hypo2_daily_portfolio')

daily.to_csv('hypo1_daily.csv')
# =============================================================================
# calculate portfolio return and plot --weekly
# =============================================================================
longSide_weekly = groupedweekly_long.cumsum()
shortSide_weekly = groupedweekly_short.cumsum()
portfolio_weekly = longSide_weekly + shortSide_weekly

dic = {'LongSide_weekly':longSide_weekly,'ShortSide_weekly':shortSide_weekly,
       'Portfolio_weekly':portfolio_weekly,'dates':longSide_weekly.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
weekly = pd.DataFrame(data = dic)

weekly.dates = pd.to_datetime(weekly['dates'],format = '%Y-%m-%d' )
weekly.set_index(['dates'],inplace = True)
weekly.plot(grid = True, title = 'Analyst Dispersion Hypo2_weekly')
weekly.Portfolio_weekly.plot(grid = True, title = 'Analyst Dispersion Hypo2_weekly_portfolio')
weekly.to_csv('hypo1_weekly.csv')

# =============================================================================
# calculate portfolio return and plot --monthly
# =============================================================================
longSide_monthly = groupedmonthly_long.cumsum()
shortSide_monthly = groupedmonthly_short.cumsum()
portfolio_monthly = longSide_monthly + shortSide_monthly

dic = {'LongSide_monthly':longSide_monthly,'ShortSide_monthly':shortSide_monthly,
       'Portfolio_monthly':portfolio_monthly,'dates':longSide_monthly.index}
##craete final dataframe which contains longside,shortside and portfolio cumulated sum return
monthly = pd.DataFrame(data = dic)

monthly.dates = pd.to_datetime(monthly['dates'],format = '%Y-%m-%d' )
monthly.set_index(['dates'],inplace = True)
monthly.plot(grid = True, title = 'Analyst Dispersion Hypo2_monthly')
monthly.Portfolio_monthly.plot(grid = True, title = 'Analyst Dispersion Hypo2_monthly_portfolio')
monthly.to_csv('hypo1_monthly.csv')

# =============================================================================
# long the top decile and short the universe benchmark
# STRATEGY = 'smooth280daywithADR_EW'
# =============================================================================
sql_universe = "SELECT * FROM [LIANGPI].[dbo].[POSITION_RETURNS] \
WHERE STRATEGY = 'smooth280daywithADR_EW' ORDER BY ENTRY_DATE"
df_universe = pd.read_sql(sql_universe,con)
df_universe = df_universe[:-11]
df_universe.ENTRY_DATE =pd.to_datetime(df_universe.ENTRY_DATE,format = '%Y-%m-%d' )
df_universe.set_index(['ENTRY_DATE'],inplace = True)

### check the difference dates 
#days_universe = df_universe['ENTRY_DATE'].unique()
#l1 = df_universe['ENTRY_DATE'].tolist()
#l2 = days.tolist()
#l3 = [x for x in l2 if x not in l1]
#print(l3)

## show the difference between benchmark and long only strategy 
universe_daily = df_universe['DAILY_RETURN'].cumsum()
difference_monthly = monthly['LongSide_monthly'] - universe_daily

hypo1_monthly = pd.concat([universe_daily,monthly['LongSide_monthly'],difference_monthly],
                axis = 1)
hypo1_monthly.columns  = ['universe_daily','longside_monthly','difference_monthly']

hypo1_monthly.plot(subplots = True)



universe_daily = df_universe['DAILY_RETURN'].cumsum()
universe_daily.to_csv('universe_return.csv')
difference_daily = daily['LongSide_daily'] - universe_daily

hypo1_daily = pd.concat([daily.Portfolio_daily,difference_daily],
                axis = 1)
hypo1_daily.columns  = ['hypo1_2ndHalf_daily','hypo1_difference_daily']

hypo1_daily.plot()


universe_daily = df_universe['DAILY_RETURN'].cumsum()
difference_weekly = weekly['LongSide_weekly'] - universe_daily

hypo1_weekly = pd.concat([universe_daily,weekly['LongSide_weekly'],difference_weekly],
                axis = 1)
hypo1_weekly.columns  = ['universe_daily','Longside_weekly','difference_weekly']

hypo1_weekly.plot(subplots = True)