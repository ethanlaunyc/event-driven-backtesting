/***************TRMI Backtest: for sentiment and emotionVSFact factors********/
--convert window timestamp column (ISO8601 to datetime)
select convert(datetime, windowTimestamp,127) as UsingConvertFrom_ISO8601, a.* into #temp1  from [LIANGPI].[INTRA\eliu].[TRMI] a 
select convert(date,UsingConvertFrom_ISO8601) as dates,a.* into #temp2 from #temp1 a 
select * from #temp2


select * from #temp4
--join tables
select a.*,b.permID,companyName,countryOfDomicile,TRBCEconomicSector,status,RIC,marketMIC into #temp3
from #temp2 a 
join [TRMIMap] b 
on a.assetCode = b.permID

--remove duplicate rows
select distinct * into #temp4 from #temp3

--generate nextTD column ,this date is the position date
select a.*,dbo.NextTD(a.dates) as next_dates into #temp5 from #temp4 a 
--join #temp5 and dscode_ric_map_daily
drop table #temp6
select * from #temp7 order by dates
select a.*,b.dscode into #temp6
from #temp5 a 
join [LIANGPI].[dbo].[Dscode_Ric_Map_Daily] b
on a.dates = b.dates and a.RIC = b.RIC


--join with universe table
select * into #universe from [LIANGPI].[dbo].[UniverseAlldates] where universename = 'smooth280daywithADR'

drop table #temp8
select a.*,b.universename into #temp7
from #temp6 a 
join #universe b
on a.dates = b.dates and a.dscode = b.dscode

select * from #temp7 order by dates,dscode
--join return
--****use next date and lead 2day return to make sure position date(the day we get factor) is one day before return date
drop table #temp8
select a.*, b.lead_2day_return into #temp8 
from #temp7 a
join MD2_lead_2days_return b
on a.dates = b.dates and a.dscode = b.dscode

select * from #t3 order by dates,buzz

--generate tables for backtesting
---now the next_dates and lead_2day_return column only have 1 day lag.
---So from here I delete the original date column, rename next_dates to date and rename lead_2day_return to lead_1day_return to disambiguation
alter table #temp8
drop column dates
select* into TRMI_processed from #temp8 
exec sp_rename 'TRMI_processed.next_dates','dates','column'
exec sp_rename 'TRMI_processed.lead_2day_return','lead_1day_return','column'

drop table TRMI_processed
select * into TRMI_News 
from TRMI_processed where datatype = 'News'

select * into TRMI_Social
from TRMI_processed where datatype = 'Social'

select * into TRMI_News_Social
from TRMI_processed where datatype = 'News_Social'


/***********Part 1: datatype : News, buzz > 20***********/
drop table #t1
select * into #t1  from TRMI_News 
where buzz > 20


--calculate zscore for sentiment and emotionVSfact columns
drop table #t3
select (sentiment - avg(sentiment) over (partition by dates order by dscode)) as numerator_sentiment,
stdev(sentiment) over(partition by dates order by dscode) as std_sentiment,
(emotionVSfact - avg(emotionVSfact) over (partition by dates order by dscode)) as numerator_emotionVSfact,
stdev(emotionVSfact) over(partition by dates order by dscode) as std_emotionVSfact,a.*
into #t2
from #t1 a 

delete from #t2 where std_sentiment =0 or std_emotionVSfact= 0

--generate zscore table
select a.*, numerator_sentiment/std_sentiment as zscore_sentiment,
numerator_emotionVSfact/std_emotionVSfact as zscore_emotionVSfact
into #t3
from #t2 a 
delete from #t3 where zscore_sentiment is null or zscore_emotionVSfact is null

select * from #t3  where dates = '2015-10-22' order by zscore_emotionVSfact
drop table News

--generate News table for backtesting which only has 5 columns 
select dates,dscode,zscore_sentiment,zscore_emotionVSfact,lead_1day_return into News from #t3

/***********Part 2: datatype : Social, buzz > 20***********/
drop table #t1
select * into #t4  from TRMI_Social
where buzz > 20


--calculate zscore for sentiment and emotionVSfact columns
drop table #t3
select (sentiment - avg(sentiment) over (partition by dates order by dscode)) as numerator_sentiment,
stdev(sentiment) over(partition by dates order by dscode) as std_sentiment,
(emotionVSfact - avg(emotionVSfact) over (partition by dates order by dscode)) as numerator_emotionVSfact,
stdev(emotionVSfact) over(partition by dates order by dscode) as std_emotionVSfact,a.*
into #t5
from #t4 a 

delete from #t5 where std_sentiment =0 or std_emotionVSfact= 0

--generate zscore table
select a.*, numerator_sentiment/std_sentiment as zscore_sentiment,
numerator_emotionVSfact/std_emotionVSfact as zscore_emotionVSfact
into #t6
from #t5 a 
delete from #t6 where zscore_sentiment is null or zscore_emotionVSfact is null

drop table Social
select dates,dscode,zscore_sentiment,zscore_emotionVSfact,lead_1day_return into Social from #t6
select * from #t6 order by dates,dscode



/***********Part 3: datatype : News & Social, buzz > 40***********/
select *  from TRMI_News_Social
where buzz > 40


--calculate zscore for sentiment and emotionVSfact columns
select (sentiment - avg(sentiment) over (partition by dates order by dscode)) as numerator_sentiment,
stdev(sentiment) over(partition by dates order by dscode) as std_sentiment,
(emotionVSfact - avg(emotionVSfact) over (partition by dates order by dscode)) as numerator_emotionVSfact,
stdev(emotionVSfact) over(partition by dates order by dscode) as std_emotionVSfact,a.*
into #t8
from #t7 a 

delete from #t8 where std_sentiment =0 or std_emotionVSfact= 0

--generate zscore table
select a.*, numerator_sentiment/std_sentiment as zscore_sentiment,
numerator_emotionVSfact/std_emotionVSfact as zscore_emotionVSfact
into #t9
from #t8 a 
delete from #t9 where zscore_sentiment is null or zscore_emotionVSfact is null

drop table Social
select dates,dscode,zscore_sentiment,zscore_emotionVSfact,lead_1day_return into News_Social from #t9
select * from TRMI_News_Social where dates = '2016-11-11' order by sentiment


/************Since zscore version doesn't work, I choose to use raw score for weights*******/
drop table News_Social
delete from TRMI_News_Social where sentiment is null or emotionVSfact is null
select dates,dscode,sentiment,emotionVSfact,lead_1day_return into News_Social_buzz20 from TRMI_News_Social where buzz > 20
select * from News_Social_buzz20
drop table news
delete from TRMI_News where sentiment is null or emotionVSfact is null
select dates,dscode,sentiment,emotionVSfact,lead_1day_return into News_buzz40 from TRMI_News where buzz > 40

drop table Social
delete from TRMI_Social where sentiment is null or emotionVSfact is null
select dates,dscode,sentiment,emotionVSfact,lead_1day_return into Social from TRMI_Social where buzz > 20


select * from News_Social_buzz20 order by dates,dscode
select * from News_Social order by dates,dscode


---run news_social where buzz >60 and 80
select dates,dscode,sentiment,emotionVSfact,lead_1day_return into News_Social_buzz60 from TRMI_News_Social where buzz > 60
select dates,dscode,sentiment,emotionVSfact,lead_1day_return into News_Social_buzz80 from TRMI_News_Social where buzz > 80
select * from News_Social_buzz60 order by dates,dscode

select dates,dscode,sentiment,emotionVSfact,lead_1day_return into News_buzz60 from TRMI_News where buzz > 60
select dates,dscode,sentiment,emotionVSfact,lead_1day_return into News_buzz80 from TRMI_News where buzz > 80


  /******News_Social_buzz20 emotionVSFact **********/
 drop table #b2
select a.*, emotionVSFact - avg(emotionVSFact) over(partition by dates ) as weighted_score into #e1 
from News_Social_buzz80 a 
select a.*,  avg(emotionVSFact) over(partition by dates ) as average into #e_1 from #e1 a 

--generate new_alphascore which make sure sum of long side equals to 1 and shortside is -1 for each day
drop table #e2
select a.*, sum(weighted_score) over (partition by dates) as new_score into #e2
from #e_1 a 
where weighted_score > 0 
union all
select a.*, sum(weighted_score) over (partition by dates)* -1 as new_score
from #e_1 a 
where weighted_score <= 0 
order by dates,dscode

select a.*, weighted_score/new_score as position into #e2a from #e2 a 



select * from #b2a order by dates,dscode
  --get the previous day and join two tables
  drop table #d3
  select a1.*,a2.position as prev_position,a2.dates as prev_dates  into #e3
  from #e2a a1
  full outer join #e2a a2
  on a2.dates = dbo.PrevTD(a1.dates) and a1.dscode = a2.dscode 
  order by dates, position

  --replace null value with 0 for current position column and previous return column)

 select isnull(position,0) current_position, isnull(prev_position,0) previous_position,dates,dscode,prev_dates,position,prev_position
 into #e4
 from #e3

 select * from #a4 order by dates,previous_position
   --calculate the turnover for each trading day
 select dates, (sum(abs(current_position- previous_position))) as turnover_social_emotionVSFact
 from #e4
 group by dates
 order by dates