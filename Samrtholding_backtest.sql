--steps for backtesting  0928
--- join two tables ds_cusip and universe---
  select a.*, b.Cusip into tb0928_1 FROM [LIANGPI].[dbo].[UniverseAlldates] a
  join Dscode_Cusip_Map_Daily b
  on a.Dates = b.Dates and a.Dscode = b.Dscode
  where a.universename = 'smooth280daywithADR'
  
--select left([tb0928_1].cusip,8) as new_cusip, tb0926.Dscode,tb0926.Dates into tb0928_2 from [tb0928_1]
--keep the left 8 digits of cusip in order to join with other columns with 8-digits cusip 
select left([tb0928_1].cusip,8) as new_cusip, [tb0928_1].*   into [tb0928_2] from [tb0928_1]

-- join Smartholdings table and universe table
select a.*, b.as_of_date,security_id,then_cusip_sedol,then_ticker,smartholdings_country_rank,smartholdings_sector_rank,smartholdings_industry_rank into tb0928_3
from [LIANGPI].[INTRA\eliu].[tb0928_2] a 
join [StarMine_Smart_Holdings_Daily] b
on a.dates = b.as_of_date and a.new_cusip = b.then_cusip_sedol

--check if there're null values in factors column, then delete null values.
select * from tb0928_3 where smartholdings_country_rank is null or smartholdings_industry_rank is null or smartholdings_sector_rank is null
delete from tb0928_3 where smartholdings_country_rank is null or smartholdings_industry_rank is null or smartholdings_sector_rank is null

---Normaliaztion of Ranks---

--declare the variables
  declare @mean_country as decimal(7,3)
  declare @std_country as decimal(7,3)
  declare @mean_sector as decimal(7,3)
  declare @std_sector as decimal(7,3)
  declare @mean_industry as decimal(7,3)
  declare @std_industry as decimal(7,3)

  -- set the variables
  set @mean_country = (select avg(smartholdings_country_rank) from [tb0928_3])
  set @std_country  = (select stdev(smartholdings_country_rank) from [tb0928_3])
  set @mean_sector  = (select avg(smartholdings_sector_rank) from [tb0928_3])
  set @std_sector   = (select stdev(smartholdings_sector_rank) from [tb0928_3])
  set @mean_industry= (select avg(smartholdings_industry_rank) from [tb0928_3])
  set @std_industry = (select stdev(smartholdings_industry_rank) from [tb0928_3])

  -- perform the normalization
  select smartholdings_country_rank,
  (smartholdings_country_rank - @mean_country)/@std_country as Normalized_country,
  smartholdings_sector_rank,
  (smartholdings_sector_rank - @mean_sector)/@std_sector as Normalized_sector,
  smartholdings_industry_rank,
  (smartholdings_industry_rank - @mean_industry)/@std_industry as Normalized_industry,
  dates, dscode into #temp_factor_Zscore1
  from [tb0928_3]
  group by dates
  order by dates

  select * from #temp_factor_Zscore1

-- bulk insert country_rank dataframe(csv) with z-score calculated by python
BULK INSERT LIANGPI.[intra\eliu].[0928_zscore_countryrank] FROM '\\lospliangpisql1\ELIU\\test3.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

-- lead factors and get positions table 
select lead(zscore_countryrank,1) over (partition by dscode order by dates) lead_zscore,
lead(smartholdings_country_rank,1) over (partition by dscode order by dates) lead_countryrank,
dates,dscode,new_cusip into positions_0928
from [0928_zscore_countryrank] order by dates,dscode
select * from positions_0928 order by dates,lead_zscore


-- bulk insert position_  rank tables
BULK INSERT LIANGPI.[intra\eliu].[positions_SectorRank] FROM '\\lospliangpisql1\ELIU\\positions_SectorRank.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[position_Country_Rank] FROM '\\lospliangpisql1\ELIU\\positions_CountryRank.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[position_Industry_Rank] FROM '\\lospliangpisql1\ELIU\\positions_IndustryRank.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

---generate new table which contains new_return, new_dates and new_dscode
select MD2_timeseries.Dates as new_dates, MD2_timeseries.Dscode as new_dscode, MD2_timeseries.[returns] as new_returns into tb1001_returns from MD2_timeseries



-- Liang's code to compute returns from positions
select M2.dates,'LP6_ETF_123y_OPT2_2Sec_LP' , 2*sum(POSITION*returns) /sum(abs(POSITION)),null
from [LIANGPI].[dbo].[POSITIONSTEST](nolock)  M1
join [LIANGPI].[dbo].[MD2_timeseries](nolock) M2 on (M1.STRATEGY='LP6_ETF_123y_OPT2_2Sec' 
 and ENTRY_DATE>'16-Sep-2007' and
M1.Dscodes=M2.Dscode and M1.ENTRY_DATE
=[LIANGPI].[dbo].PrevTD(dates)) 
group by M2.dates



--- lead returns table
select lead(new_returns,1) over (partition by new_dscode order by new_dates) lead_1day_return,
new_dates,new_dscode into lead_1day_returns
from [tb1001_returns] order by new_dates, new_dscode
--check
select top(10000) lead_2day_return, new_dates, new_dscode from lead_2day_returns where new_dates > '2004-01-06'
order by new_dates, new_dscode

--- lag three position tables( country, sector, industry)
select lead(all_positions,1) over (partition by dscode order by dates) lead_positions,
dates, dscode into TB_Positions_Country
 from [Position_Country_Rank] order by dates, dscode

 select lead(all_positions,1) over (partition by dscode order by dates) lead_positions,
dates, dscode into TB_Positions_Sector
 from [Positions_SectorRank] order by dates, dscode

 select lead(all_positions,1) over (partition by dscode order by dates) lead_positions,
dates, dscode into TB_Positions_Industry
 from [Position_Industry_Rank] order by dates, dscode

-- EXEC sp_rename 'Positions_SectorRank','position_Sector_Rank' 


--- lag three position tables( country, sector, industry)
select lead(all_positions,1) over (partition by dscode order by dates) lead_positions,
dates, dscode into TB_Positions_Country
 from [Position_Country_Rank] order by dates, dscode

 select lead(all_positions,1) over (partition by dscode order by dates) lead_positions,
dates, dscode into TB_Positions_Sector
 from [Positions_SectorRank] order by dates, dscode

 select lead(all_positions,1) over (partition by dscode order by dates) lead_positions,
dates, dscode into TB_Positions_Industry
 from [Position_Industry_Rank] order by dates, dscode

-- EXEC sp_rename 'Positions_SectorRank','position_Sector_Rank'

---join positions table(country ,sector, industry rank) and returns talbe, generate portfolio return
select a.dates, sum(a.[lead_positions] * b.[lead_2day_return]) as port_return into portfolio_CountryRank
from [LIANGPI].[INTRA\eliu].[TB_Positions_Country] a
join lead_2day_returns b
on a.dates = b.new_dates and a.dscode = b.new_dscode
group by a.dates


select a.dates, sum(a.[lead_positions] * b.[lead_2day_return]) as port_return into portfolio_SectorRank
from [LIANGPI].[INTRA\eliu].TB_Positions_Sector a
join lead_2day_returns b
on a.dates = b.new_dates and a.dscode = b.new_dscode
group by a.dates


select a.dates, sum(a.[lead_positions] * b.[lead_2day_return]) as port_return into portfolio_IndustryRank
from [LIANGPI].[INTRA\eliu].[TB_Positions_Industry] a
join lead_2day_returns b
on a.dates = b.new_dates and a.dscode = b.new_dscode
group by a.dates
