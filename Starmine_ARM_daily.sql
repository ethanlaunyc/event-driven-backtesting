/*****  backtesting of SmartHolding_ARM_daily (analyst revision model) factors ***/

--preprocessing the  data--
--- join two tables ds_cusip and universe---
  select a.*, b.Cusip into #temp1 FROM [LIANGPI].[dbo].[UniverseAlldates] a
  join Dscode_Cusip_Map_Daily b
  on a.Dates = b.Dates and a.Dscode = b.Dscode
  where a.universename = 'smooth280daywithADR'

--keep the left 8 digits of cusip in order to join with other columns with 8-digits cusip 
select left(#temp1.cusip,8) as new_cusip, #temp1.dates,dscode,universename  into #temp2 from #temp1

-- join Smartholding_ARM_Daily table and universe table
-- seperate the backtesting into 2 parts( since ARM_Secondary has 1/10 null values and will influence the results if just delete all of them)
select a.*, b.Data_Through_Date,then_cusip_sedol,ARM_preferred_earnings_component,ARM_revenue_component,
ARM_recommendations_component,ARM_ex_recommendations into #temp3
from [LIANGPI].[INTRA\eliu].#temp2 a 
join [StarMine_ARM_Daily] b
on a.dates = b.Data_Through_Date and a.new_cusip = b.then_cusip_sedol



/*****  this part is for dealing with [ARM_secondary_earnings_compunent] column, which cntains 1/10 null values totally ***/
select a.*, b.Data_Through_Date,then_cusip_sedol,ARM_preferred_earnings_component,ARM_secondary_earnings_compunent,ARM_revenue_component,
ARM_recommendations_component,ARM_ex_recommendations into #secondary1
from [LIANGPI].[INTRA\eliu].#temp2 a 
join [StarMine_ARM_Daily] b
on a.dates = b.Data_Through_Date and a.new_cusip = b.then_cusip_sedol

--check if there're null values in factors column, then delete null values.
delete from  #secondary1 where ARM_preferred_earnings_component is null  or ARM_revenue_component is null
or ARM_recommendations_component is null  or ARM_ex_recommendations is null or ARM_secondary_earnings_compunent is null

---Normaliaztion of factors(ranks)---
select (ARM_secondary_earnings_compunent-avg(ARM_secondary_earnings_compunent) over(partition by #secondary1.dates order by dscode )) as mean_secodnary,
stdev(ARM_secondary_earnings_compunent) over(partition by #secondary1.dates order by dscode) as std_secondary,#secondary1.*
into #secondary2
from #secondary1

delete  from #secondary2 where std_secondary = '0' 

--generate zscore table
 select mean_secodnary/std_secondary as zscore_secondary,#secondary2.*
 into #secondary3 from #secondary2

 ---join lead return table with #zscore table
 select a.*, b.lead_2day_return into #secondary4 from #secondary3 a 
join MD2_lead_2days_return b on a.dates = b.dates and a.dscode = b.dscode  

 select * from #secondary4

--generate a,b column which means the exact number of top/bottom decile for each day ( for 4 factors, generate 4 ARM tables)
select dates,dscode,zscore_secondary,ARM_secondary_earnings_compunent, lead_2day_return into #secondary5 from #secondary4  
order by dates,zscore_secondary

select dates,dscode,zscore_secondary,ARM_secondary_earnings_compunent,lead_2day_return,
percentile_cont(0.1) within group (order by zscore_secondary) over (partition by dates) as a,
percentile_cont(0.9) within group (order by zscore_secondary) over (partition by dates) as b
into ARM_secondary
from #secondary5
 order by dates,dscode

-- EXEC sp_RENAME 'ARM_secondary.ARM_secondary_earnings_compunent' , 'ARM_secondary_earnings_component', 'COLUMN'
 select * from ARM_secondary order by dates,zscore_secondary







--drop table #temp4
--check if there're null values in factors column, then delete null values.
select * from  #temp3 where ARM_preferred_earnings_component is null  or ARM_revenue_component is null
or ARM_recommendations_component is null  or ARM_ex_recommendations is null 

delete from  #temp3 where ARM_preferred_earnings_component is null  or ARM_revenue_component is null
or ARM_recommendations_component is null  or ARM_ex_recommendations is null 


select * from #temp3

---Normaliaztion of factors(ranks)---
select (ARM_preferred_earnings_component-avg(ARM_preferred_earnings_component) over(partition by #temp3.dates order by dscode )) as mean_preferred,
stdev(ARM_preferred_earnings_component) over(partition by #temp3.dates order by dscode) as std_preferred,

(ARM_revenue_component-avg(ARM_revenue_component) over(partition by #temp3.dates order by dscode )) as mean_revenue,
stdev(ARM_revenue_component) over(partition by #temp3.dates order by dscode) as std_revenue, 

(ARM_recommendations_component-avg(ARM_recommendations_component) over(partition by #temp3.dates order by dscode )) as mean_recommendations,
stdev(ARM_recommendations_component) over(partition by #temp3.dates order by dscode) as std_recommendations, 

(ARM_ex_recommendations-avg(ARM_ex_recommendations) over(partition by #temp3.dates order by dscode )) as mean_ex_recommendations,
stdev(ARM_ex_recommendations) over(partition by #temp3.dates order by dscode) as std_ex_recommendations, #temp3.*

into #temp4
from #temp3
--delete "std values = 0" so that we can calculate zscore
select * from #temp4 where std_preferred = '0' or std_revenue = '0' or std_recommendations = '0' or std_ex_recommendations = '0'
delete  from #temp4 where std_preferred = '0' or std_revenue = '0' or std_recommendations = '0' or std_ex_recommendations = '0'

--- lead 2 days return table
select lead(returns,2) over (partition by dscode order by dates) lead_2day_return,
dates,dscode into MD2_lead_2days_return from [LIANGPI].[dbo].[MD2_timeseries]

--generate zscore table
 select mean_preferred/std_preferred as zscore_preferred, mean_revenue/std_revenue as zscore_revenue, 
 mean_recommendations/std_recommendations as zscore_recommendations,mean_ex_recommendations/std_ex_recommendations as zscore_ex_recommendations,#temp4.*
 into #temp5 from #temp4


 ---join lead return table with #zscore table
select a.*, b.lead_2day_return into Starmine_ADR_normalized from #temp5 a 
join MD2_lead_2days_return b on a.dates = b.dates and a.dscode = b.dscode  
--drop table #temp6
select *from Starmine_ADR_normalized order by dates,zscore_preferred

--generate a,b column which means the exact number of top/bottom decile for each day ( for 4 factors, generate 4 ARM tables)
select dates,dscode,zscore_preferred,ARM_preferred_earnings_component, lead_2day_return into #preferred from #temp6  order by dates,zscore_preferred

select dates,dscode,zscore_preferred,ARM_preferred_earnings_component,lead_2day_return,
percentile_cont(0.1) within group (order by zscore_preferred) over (partition by dates) as a,
percentile_cont(0.9) within group (order by zscore_preferred) over (partition by dates) as b
into ARM_preferred
from #preferred
 order by dates,dscode

 select * from ARM_preferred order by dates,zscore_preferred


 --revenue
 select dates,dscode,zscore_revenue,ARM_revenue_component, lead_2day_return into #revenue from #temp6  order by dates,zscore_revenue

select dates,dscode,zscore_revenue,ARM_revenue_component,lead_2day_return,
percentile_cont(0.1) within group (order by zscore_revenue) over (partition by dates) as a,
percentile_cont(0.9) within group (order by zscore_revenue) over (partition by dates) as b
into ARM_revenue
from #revenue
 order by dates,dscode

 select * from ARM_revenue order by dates,zscore_revenue


 --recommendations
 select dates,dscode,zscore_recommendations,ARM_recommendations_component, lead_2day_return into #recommendations from #temp6  order by dates,zscore_recommendations

select dates,dscode,zscore_recommendations,ARM_recommendations_component,lead_2day_return,
percentile_cont(0.1) within group (order by zscore_recommendations) over (partition by dates) as a,
percentile_cont(0.9) within group (order by zscore_recommendations) over (partition by dates) as b
into ARM_recommendations
from #recommendations
 order by dates,dscode

 select * from ARM_recommendations order by dates,zscore_recommendations

--ex_recommendations
 select dates,dscode,zscore_ex_recommendations,ARM_ex_recommendations, lead_2day_return into #ex_recommendations from #temp6  order by dates,zscore_ex_recommendations

select dates,dscode,zscore_ex_recommendations,ARM_ex_recommendations,lead_2day_return,
percentile_cont(0.1) within group (order by zscore_ex_recommendations) over (partition by dates) as a,
percentile_cont(0.9) within group (order by zscore_ex_recommendations) over (partition by dates) as b
into ARM_ex_recommendations
from #ex_recommendations
 order by dates,dscode

 select * from ARM_ex_recommendations order by dates,zscore_ex_recommendations





 --2018/10/24
 /*****  backtest ARM_Revision_score and ARM_score_5 ***/

--preprocessing the  data--
--- join two tables ds_cusip and universe---
  select a.*, b.Cusip into #temp1 FROM [LIANGPI].[dbo].[UniverseAlldates] a
  join Dscode_Cusip_Map_Daily b
  on a.Dates = b.Dates and a.Dscode = b.Dscode
  where a.universename = 'smooth280daywithADR'

--keep the left 8 digits of cusip in order to join with other columns with 8-digits cusip 
select left(#temp1.cusip,8) as new_cusip, #temp1.dates,dscode,universename  into #temp2 from #temp1

-- join Smartholding_ARM_Daily table and universe table

select a.*, b.Data_Through_Date,then_cusip_sedol,ARM_score_5,Analyst_revisions_score into #score1
from [LIANGPI].[INTRA\eliu].#temp2 a 
join [StarMine_ARM_Daily] b
on a.dates = b.Data_Through_Date and a.new_cusip = b.then_cusip_sedol


--check if there're null values in factors column, then delete null values.
select * from  #score1 where ARM_score_5 is null  or Analyst_revisions_score is null
select * from  #score1  order by dates,Analyst_revisions_score


---Normaliaztion of Analyst_revisions_score---
drop table #score2
select (Analyst_revisions_score-avg(Analyst_revisions_score) over(partition by #score1.dates order by dscode )) 
as mean_Analyst_revisions_score,
stdev(Analyst_revisions_score) over(partition by #score1.dates order by dscode) as std_Analyst_revisions_score,#score1.*
into #score2
from #score1

select *  from #score2 where std_Analyst_revisions_score = '0' 
delete  from #score2 where std_Analyst_revisions_score = '0' 

--generate zscore table
 select mean_Analyst_revisions_score/std_Analyst_revisions_score as zscore_Analyst_revisions_score,#score2.*
 into #score3 from #score2

  ---join lead return table with #score3 table

 select a.dates,a.dscode,a.zscore_Analyst_revisions_score,a.Analyst_revisions_score, b.lead_2day_return into #score4 from #score3 a 
join MD2_lead_2days_return b on a.dates = b.dates and a.dscode = b.dscode


 ---join lead return table with #zscore table
 drop table ARM_score_5
 select a.dates,a.dscode,a.arm_score_5, b.lead_2day_return into ARM_score_5 from #score1 a 
join MD2_lead_2days_return b on a.dates = b.dates and a.dscode = b.dscode  

 select * from #score4 order by dates

--generate a,b column which means the exact number of top/bottom decile for each day 
select dates,dscode,zscore_Analyst_revisions_score,Analyst_revisions_score, lead_2day_return into #score5 from #score4 
order by dates,zscore_Analyst_revisions_score

drop table ARM_Analyst_revisions
select dates,dscode,zscore_Analyst_revisions_score,Analyst_revisions_score,lead_2day_return,
percentile_cont(0.9) within group (order by zscore_Analyst_revisions_score) over (partition by dates) as a,
percentile_cont(0.1) within group (order by zscore_Analyst_revisions_score) over (partition by dates) as b
into ARM_Analyst_revisions
from #score4
 order by dates,dscode

-- EXEC sp_RENAME 'ARM_secondary.ARM_secondary_earnings_compunent' , 'ARM_secondary_earnings_component', 'COLUMN'
 select * from ARM_Analyst_revisions order by dates,zscore_Analyst_revisions_score

