/****** 3month and 12 month change of ESG_CG factor  ******/
SELECT dates,dscode,CG,zscore_CG,lead_2day_return into #t1
  FROM [LIANGPI].[INTRA\eliu].[ESG_CG2] where dates >'2009-12-31' order by dates,dscode

drop table #t1
select * from #t1  order by dates,dscode

---zscore change: 3 month change----
---generate temp lead 3 month table 
select a.*, lag(CG,60) over (partition by dscode order by dates) as lag_CG,
 lag(zscore_CG,60) over (partition by dscode order by dates) as lag_zscore,
 lag(dscode,60) over (partition by dscode order by dates) as lag_dscode,
 lag(dates,60) over (partition by dscode order by dates) as lag_dates
into #t2
from ESG_CG a 
order by dates,dscode

select * from #t2 order by dates,lag_zscore

--inner join tables
select a.*,b.lag_zscore
into #t3
from ESG_CG a
inner join #t2 b
on a.dates = b.dates and a.dscode = b.lag_dscode
order by dates,dscode

drop table ESG_CG_3M
select a.*, (zscore_CG - lag_zscore) as new_zscore
into ESG_CG_3M
from #t3 a
order by dates,dscode

select * from  ESG_CG_3M where dates = '2010-04-09' order by lag_zscore


---12 month change----
---generate temp lead 3 month table 
select a.*, lag(CG,240) over (partition by dscode order by dates) as lag_CG,
 lag(zscore_CG,240) over (partition by dscode order by dates) as lag_zscore,
 lag(dscode,240) over (partition by dscode order by dates) as lag_dscode,
 lag(dates,240) over (partition by dscode order by dates) as lag_dates
into #a2
from ESG_CG a 
order by dates,dscode

--inner join tables
select a.*,b.lag_zscore
into #a3
from ESG_CG a
inner join #a2 b
on a.dates = b.dates and a.dscode = b.lag_dscode
order by dates,dscode

select a.*, (zscore_CG - lag_zscore) as new_zscore
into ESG_CG_12M
from #a3 a
order by dates,dscode

select * from  ESG_CG_12M where dates = '2010-12-23' order by lag_zscore




---1 month change----
---generate temp lead 3 month table 
select a.*, lag(CG,22) over (partition by dscode order by dates) as lag_CG,
 lag(zscore_CG,22) over (partition by dscode order by dates) as lag_zscore,
 lag(dscode,22) over (partition by dscode order by dates) as lag_dscode,
 lag(dates,22) over (partition by dscode order by dates) as lag_dates
into #c2
from ESG_CG a 
order by dates,dscode

drop table #c3
--inner join tables
select a.*,b.lag_zscore
into #c3
from ESG_CG a
inner join #c2 b
on a.dates = b.dates and a.dscode = b.lag_dscode
order by dates,dscode

drop table ESG_CG_1M
select a.*, (zscore_CG - lag_zscore) as new_zscore
into ESG_CG_1M
from #c3 a
order by dates,dscode

select * from  ESG_CG_1M where dates = '2010-12-23' order by lag_zscore




---rank change: 3 month change----
---generate temp lead 3 month table 
select a.*, lag(CG,60) over (partition by dscode order by dates) as lag_CG,

 lag(dscode,60) over (partition by dscode order by dates) as lag_dscode

into #b2
from #t1 a 
order by dates,dscode

--inner join tables
select a.*,b.lag_CG
into #b3
from #t1 a
inner join #b2 b
on a.dates = b.dates and a.dscode = b.lag_dscode
order by dates,dscode

select a.*, (CG - lag_CG) as new_CG
into #b4
from #b3 a
order by dates,dscode

--generate new zscore
select a.*, (new_CG -AVG(new_CG) over (partition by dates order by dscode)) as mean_new_CG,
stdev(new_CG) over (partition by dates order by dscode) as new_std_CG
into #b5
from #b4 a 


 delete from #b5 where new_std_CG=0
 drop table #b6
select a.*, mean_new_CG/new_std_CG as final_zscore
into ESG_CG_Rank3M
from #b5 a 
order by dates,dscode



select * from  #b6  order by dates,dscode
