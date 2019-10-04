/***** Analyst dispersion  Hypo 1 backtest -----weekly/monthly/daily ***/
  --negative relation between analyst dispesion(FC_CVFY1EPS) and expected future return(MD2_lead_1week_return) 
  --when the stock is less covered by analyst(FC_NumEst) less than the median
 
/***** weekly test ***/

 
drop table #temp1
  --lead 1 day position factor(FC_CVFY1EPS)
  select lead(FC_CVFY1EPS,1) over (partition by dscode order by dates) lead_1day_FC_CVFY1EPS,
dates,dscode,FC_NumEst into #lead_1day_Analyst_Dispersion
from [Analyst_Dispersion] order by dates, dscode

  --generate median column in order to seperate the universe
  drop table Analyst_Dispersion_hypo1
  drop table #more_half

select a.*,
percentile_cont(0.5) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as median
into #temp1
 from [#lead_1day_Analyst_Dispersion] a
 order by dates,dscode

 select * from #temp1 order by dates,dscode


--lead 1 week MD2_return
  drop table #MD2_lead_1week_return

select lead(Returns,5) over (partition by dscode order by dates) lead_1week_return,
dates, dscode into #MD2_lead_1week_return
 from [LIANGPI].[dbo].[MD2_timeseries] order by dates, dscode

 ---join #MD2_lead_1week_return table with #temp1 (here I use daily return data)
drop table #more_half
select a.*, b.lead_1week_return into #temp4 from #temp1 a 
join #MD2_lead_1week_return b on a.dates = b.dates and a.dscode = b.dscode  

select * from #temp4 order by dates,FC_NumEst

--seperate the universe based on the median

  select * into #less_half_weekly from #temp4 where FC_NumEst <= median order by dates,FC_NumEst
   select *  into #more_half from #temp4 where FC_NumEst > median order by dates,FC_NumEst

   select * from #more_half order by dates,FC_NumEst

--Since dispersion factor has negative relation with expected return when the Ananyst converge facor is less than the median,
--I hereby short the top decile of FC_CCFY1EPS and long bottom decile FC_CCFY1EPS


--delete null values in the table first  before calculate top&bottom decile
select * from #less_half_weekly where lead_1day_FC_CVFY1EPS is null
delete from #less_half_weekly where lead_1day_FC_CVFY1EPS is null


drop table Analyst_Dispersion_hypo1_weekly
--generate a,b column which means the top/bottom decile of [FC_CVFY1EPS] column for each day
select dates,dscode,lead_1day_FC_CVFY1EPS,lead_1week_return,median,
percentile_cont(0.1) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as a,
percentile_cont(0.9) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as b
into Analyst_Dispersion_hypo1_weekly_2nd
 from #less_half_weekly
 order by dates,dscode

select * from Analyst_Dispersion_hypo1_weekly order by dates,lead_1day_FC_CVFY1EPS
exec sp_rename 'Analyst_Dispersion_hypo1_weekly_2nd.lead_1day_FC_CVFY1EPS','lead_1day_position','column'
-- select distinct dates from Analyst_Dispersion_hypo1_monthly order by dates




/***** monthly test ***/


drop table #temp1
  --lead 1 day position factor(FC_CVFY1EPS)
  select lead(FC_CVFY1EPS,1) over (partition by dscode order by dates) lead_1day_FC_CVFY1EPS,
dates,dscode,FC_NumEst into #lead_1day_Analyst_Dispersion
from [Analyst_Dispersion] order by dates, dscode

  --generate median column in order to seperate the universe
  drop table Analyst_Dispersion_hypo1
  drop table #more_half

select a.*,
percentile_cont(0.5) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as median
into #temp1
 from [#lead_1day_Analyst_Dispersion] a
 order by dates,dscode

 select * from #temp1 order by dates,dscode


--lead 1 week MD2_return
  drop table #MD2_lead_1week_return

select lead(Returns,22) over (partition by dscode order by dates) lead_1month_return,
dates, dscode into #MD2_lead_1month_return
 from [LIANGPI].[dbo].[MD2_timeseries] order by dates, dscode

 ---join MD2_lead_1month_return table with #temp1 (here I use daily return data)
drop table #temp2
drop table #more_half

select a.*, b.lead_1month_return into #temp2 from #temp1 a 
join #MD2_lead_1month_return b on a.dates = b.dates and a.dscode = b.dscode  

select * from #temp2 order by dates,FC_NumEst

--seperate the universe based on the median

  select * into #less_half_monthly from #temp2 where FC_NumEst <= median order by dates,FC_NumEst
   select *  into #more_half from #temp2 where FC_NumEst > median order by dates,FC_NumEst

   select * from #more_half order by dates,FC_NumEst

--Since dispersion factor has negative relation with expected return when the Ananyst converge facor is less than the median,
--I hereby short the top decile of FC_CCFY1EPS and long bottom decile FC_CCFY1EPS


--delete null values in the table first  before calculate top&bottom decile
select * from #less_half_monthly where lead_1day_FC_CVFY1EPS is null
delete from #less_half_monthly where lead_1day_FC_CVFY1EPS is null


drop table Analyst_Dispersion_hypo1_daily
--generate a,b column which means the top/bottom decile of [FC_CVFY1EPS] column for each day
select dates,dscode,lead_1day_FC_CVFY1EPS,lead_1month_return,median,
percentile_cont(0.1) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as a,
percentile_cont(0.9) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as b
into Analyst_Dispersion_hypo1_monthly_2nd
 from #less_half_monthly
 order by dates,dscode

select * from Analyst_Dispersion_hypo1_weekly order by dates,lead_1day_FC_CVFY1EPS

-- select distinct dates from Analyst_Dispersion_hypo1_monthly order by dates

delete from Analyst_Dispersion_hypo1_weekly where lead_1day_position is null

exec sp_rename 'Analyst_Dispersion_hypo1_monthly_2nd.lead_1day_FC_CVFY1EPS','lead_1day_position','column'







/***** daily test ***/


drop table #temp1
  --lead 1 day position factor(FC_CVFY1EPS)
  select lead(FC_CVFY1EPS,1) over (partition by dscode order by dates) lead_1day_FC_CVFY1EPS,
dates,dscode,FC_NumEst into #lead_1day_Analyst_Dispersion
from [Analyst_Dispersion] order by dates, dscode

  --generate median column in order to seperate the universe
  drop table Analyst_Dispersion_hypo1
  drop table #more_half

select a.*,
percentile_cont(0.5) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as median
into #temp1
 from [#lead_1day_Analyst_Dispersion] a
 order by dates,dscode

 select * from #temp1 order by dates,dscode




 ---join MD2_lead_2days_return table with #temp1 (here I use daily return data)
drop table #temp2
drop table #more_half
select a.*, b.lead_2day_return into #temp3 from #temp1 a 
join MD2_lead_2days_return b on a.dates = b.dates and a.dscode = b.dscode  

select * from #temp3 order by dates,FC_NumEst

--seperate the universe based on the median

  select * into #less_half_daily from #temp3 where FC_NumEst <= median order by dates,FC_NumEst
   select *  into #more_half from #temp3 where FC_NumEst > median order by dates,FC_NumEst

   select * from #more_half order by dates,FC_NumEst

--Since dispersion factor has negative relation with expected return when the Ananyst converge facor is less than the median,
--I hereby short the top decile of FC_CCFY1EPS and long bottom decile FC_CCFY1EPS


--delete null values in the table first  before calculate top&bottom decile
select * from #less_half_daily where lead_1day_FC_CVFY1EPS is null
delete from #less_half_daily where lead_1day_FC_CVFY1EPS is null


drop table Analyst_Dispersion_hypo1_daily
--generate a,b column which means the top/bottom decile of [FC_CVFY1EPS] column for each day
select dates,dscode,lead_1day_FC_CVFY1EPS,lead_2day_return,median,
percentile_cont(0.1) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as a,
percentile_cont(0.9) within group (order by lead_1day_FC_CVFY1EPS) over (partition by dates) as b
into Analyst_Dispersion_hypo1_daily_2nd
 from #less_half_daily
 order by dates,dscode

select * from Analyst_Dispersion_hypo1_weekly order by dates,lead_1day_FC_CVFY1EPS
exec sp_rename 'Analyst_Dispersion_hypo1_daily_2nd.lead_1day_FC_CVFY1EPS','lead_1day_position','column'
-- select distinct dates from Analyst_Dispersion_hypo1_monthly order by dates



