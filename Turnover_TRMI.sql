/******Calculate turnover for TRMI, process News, Social and News_Social separately **********/
---News_sentiment----
drop table #t1
select a.*, sentiment - avg(sentiment) over(partition by dates ) as weighted_score into #t1 
from news a 
select a.*,  avg(sentiment) over(partition by dates ) as average into #t_1 from #t1 a 

--generate new_alphascore which make sure sum of long side equals to 1 and shortside is -1 for each day
drop table #t2
select a.*, sum(weighted_score) over (partition by dates) as new_score into #t2
from #t_1 a 
where weighted_score > 0 

union all

select a.*, sum(weighted_score) over (partition by dates)* -1 as new_score
from #t_1 a 
where weighted_score <= 0 
order by dates,dscode

select a.*, weighted_score/new_score as position into #t2a from #t2 a 



select * from #t2a order by dates,dscode
  --get the previous day and join two tables
  drop table #t4
  select a1.*,a2.position as prev_position,a2.dates as prev_dates  into #t3
  from #t2a a1
  full outer join #t2a a2
  on a2.dates = dbo.PrevTD(a1.dates) and a1.dscode = a2.dscode 
  order by dates, position

  --replace null value with 0 for current position column and previous return column)

 select isnull(position,0) current_position, isnull(prev_position,0) previous_position,dates,dscode,prev_dates,position,prev_position
 into #t4
 from #t3

 select * from #t4 order by dates,previous_position
   --calculate the turnover for each trading day
 select dates, (sum(abs(current_position- previous_position))) as turnover_news_sentiment
 from #t4
 group by dates
 order by dates

 /******News_emotionVSFact **********/
select a.*, emotionVSfact - avg(emotionVSfact) over(partition by dates ) as weighted_score into #a1 
from news_buzz40 a 
select a.*,  avg(emotionVSfact) over(partition by dates ) as average into #a_1 from #a1 a 

--generate new_alphascore which make sure sum of long side equals to 1 and shortside is -1 for each day
drop table #a2
select a.*, sum(weighted_score) over (partition by dates) as new_score into #a2
from #a_1 a 
where weighted_score > 0 

union all

select a.*, sum(weighted_score) over (partition by dates)* -1 as new_score
from #a_1 a 
where weighted_score <= 0 
order by dates,dscode

select a.*, weighted_score/new_score as position into #a2a from #a2 a 



select * from #a2a order by dates,dscode
  --get the previous day and join two tables
  drop table #a4
  select a1.*,a2.position as prev_position,a2.dates as prev_dates  into #a3
  from #a2a a1
  full outer join #a2a a2
  on a2.dates = dbo.PrevTD(a1.dates) and a1.dscode = a2.dscode 
  order by dates, position

  --replace null value with 0 for current position column and previous return column)

 select isnull(position,0) current_position, isnull(prev_position,0) previous_position,dates,dscode,prev_dates,position,prev_position
 into #a4
 from #a3

 select * from #a4 order by dates,previous_position
   --calculate the turnover for each trading day
 select dates, (sum(abs(current_position- previous_position))) as turnover_news_sentiment
 from #a4
 group by dates
 order by dates

  /******Social_emotionVSFact **********/
 drop table #b2
select a.*, emotionVSfact - avg(emotionVSfact) over(partition by dates ) as weighted_score into #b1 
from Social a 
select a.*,  avg(emotionVSfact) over(partition by dates ) as average into #b_1 from #b1 a 

--generate new_alphascore which make sure sum of long side equals to 1 and shortside is -1 for each day

select a.*, sum(weighted_score) over (partition by dates) as new_score into #b2
from #b_1 a 
where weighted_score > 0 

union all

select a.*, sum(weighted_score) over (partition by dates)* -1 as new_score
from #b_1 a 
where weighted_score <= 0 
order by dates,dscode

select a.*, weighted_score/new_score as position into #b2a from #b2 a 



select * from #b2a order by dates,dscode
  --get the previous day and join two tables
  drop table #b3
  select a1.*,a2.position as prev_position,a2.dates as prev_dates  into #b3
  from #b2a a1
  full outer join #b2a a2
  on a2.dates = dbo.PrevTD(a1.dates) and a1.dscode = a2.dscode 
  order by dates, position

  --replace null value with 0 for current position column and previous return column)

 select isnull(position,0) current_position, isnull(prev_position,0) previous_position,dates,dscode,prev_dates,position,prev_position
 into #b4
 from #b3

 select * from #a4 order by dates,previous_position
   --calculate the turnover for each trading day
 select dates, (sum(abs(current_position- previous_position))) as turnover_social_emotionVSFact
 from #b4
 group by dates
 order by dates

 
  /******Social_sentiment **********/
 drop table #b2
select a.*, sentiment - avg(sentiment) over(partition by dates ) as weighted_score into #c1 
from Social a 
select a.*,  avg(sentiment) over(partition by dates ) as average into #c_1 from #c1 a 

--generate new_alphascore which make sure sum of long side equals to 1 and shortside is -1 for each day

select a.*, sum(weighted_score) over (partition by dates) as new_score into #c2
from #c_1 a 
where weighted_score > 0 

union all

select a.*, sum(weighted_score) over (partition by dates)* -1 as new_score
from #c_1 a 
where weighted_score <= 0 
order by dates,dscode

select a.*, weighted_score/new_score as position into #c2a from #c2 a 



select * from #b2a order by dates,dscode
  --get the previous day and join two tables
  drop table #b3
  select a1.*,a2.position as prev_position,a2.dates as prev_dates  into #c3
  from #c2a a1
  full outer join #c2a a2
  on a2.dates = dbo.PrevTD(a1.dates) and a1.dscode = a2.dscode 
  order by dates, position

  --replace null value with 0 for current position column and previous return column)

 select isnull(position,0) current_position, isnull(prev_position,0) previous_position,dates,dscode,prev_dates,position,prev_position
 into #c4
 from #c3

 select * from #a4 order by dates,previous_position
   --calculate the turnover for each trading day
 select dates, (sum(abs(current_position- previous_position))) as turnover_social_emotionVSFact
 from #c4
 group by dates
 order by dates

   /******News_Social sentiment **********/
 drop table #b2
select a.*, sentiment - avg(sentiment) over(partition by dates ) as weighted_score into #d1 
from News_Social a 
select a.*,  avg(sentiment) over(partition by dates ) as average into #d_1 from #d1 a 

--generate new_alphascore which make sure sum of long side equals to 1 and shortside is -1 for each day

select a.*, sum(weighted_score) over (partition by dates) as new_score into #d2
from #d_1 a 
where weighted_score > 0 

union all

select a.*, sum(weighted_score) over (partition by dates)* -1 as new_score
from #d_1 a 
where weighted_score <= 0 
order by dates,dscode

select a.*, weighted_score/new_score as position into #d2a from #d2 a 



select * from #b2a order by dates,dscode
  --get the previous day and join two tables
  drop table #d3
  select a1.*,a2.position as prev_position,a2.dates as prev_dates  into #d3
  from #d2a a1
  full outer join #d2a a2
  on a2.dates = dbo.PrevTD(a1.dates) and a1.dscode = a2.dscode 
  order by dates, position

  --replace null value with 0 for current position column and previous return column)

 select isnull(position,0) current_position, isnull(prev_position,0) previous_position,dates,dscode,prev_dates,position,prev_position
 into #d4
 from #d3

 select * from #a4 order by dates,previous_position
   --calculate the turnover for each trading day
 select dates, (sum(abs(current_position- previous_position))) as turnover_social_emotionVSFact
 from #d4
 group by dates
 order by dates
 Exec sp_defaultdb @loginame='INTRA\eliu', @defdb='LIANGPI' 


    /******News_Social_buzz20 emotionVSFact **********/
 drop table #b2
select a.*, emotionVSFact - avg(emotionVSFact) over(partition by dates ) as weighted_score into #e1 
from News_Social_buzz60 a 
select a.*,  avg(emotionVSFact) over(partition by dates ) as average into #e_1 from #e1 a 

--generate new_alphascore which make sure sum of long side equals to 1 and shortside is -1 for each day
drop table #e3
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