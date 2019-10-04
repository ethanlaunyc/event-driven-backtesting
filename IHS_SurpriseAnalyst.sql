/****** IHS Surpirse Analyst, 5 factors  ******/
SELECT * into #t1
  FROM [LIANGPI].[INTRA\eliu].[IHS_SurpriseAnalyst] where date>'2008-12-31' order by date,QSA_Composite

--join with dscode_cusip
SELECT * from IHS_SurpriseAnalyst_QSA_Composite  order by dates,dscode

--keep the left 8 digits of cusip in order to join with other columns with 8-digits cusip 
select left(cusip,8) as new_cusip, a.*  into #temp from Dscode_Cusip_Map_Daily a 

select a.*,b.dscode into #t2 
from #t1 a 
join #temp b
on a.date = b.dates and a.cusip = b.new_cusip

--delete null values
delete from #t2 
where QSA_Composite is null or QSA_Efficiency is null or QSA_EstExpect is null or QSA_Percent is null or QSA_SurpSN is null

--join with universe talbe
drop table #t3
select a.*, b.universename into #t3
from #t2 a 
join dbo.UniverseAlldates b
on a.date = b.dates and a.dscode = b.Dscode
where universename = 'smooth280daywithADR'

--join with lead 2day return table
select a.*,b.lead_2day_return into IHS_SurpriseAnalyst_cleaned
from #t3 a 
join [MD2_lead_2days_return] b
 on a.date = b.dates and a.dscode = b.dscode
 order by date,dscode

 exec sp_rename 'IHS_SurpriseAnalyst_cleaned.date','dates','column'
 SELECT * from IHS_SurpriseAnalyst_cleaned order by dates,name

 --QSA_Composite factor(surpirse analyst composite rank)
 drop table #a11
 select dates,dscode,name,QSA_Composite,lead_2day_return,
percentile_cont(0.2) within group (order by QSA_Composite) over (partition by dates) as a,
percentile_cont(0.8) within group (order by QSA_Composite) over (partition by dates) as b,
 (QSA_Composite-AVG(QSA_Composite) over (partition by dates )) as mean_QSA_Composite,
stdev(QSA_Composite) over (partition by dates ) as std_QSA_Composite
into #a11
 from IHS_SurpriseAnalyst_cleaned


 select * from #a11 order by QSA_Composite,dates,dscode
 delete from #a1 where  std_QSA_Composite = 0
 drop table IHS_SurpriseAnalyst_QSA_Composite
 select a.*, mean_QSA_Composite/std_QSA_Composite as zscore_QSA_Composite into IHS_SurpriseAnalyst_QSA_Composite
 from #a11 a 
 order by dates,dscode

 SELECT * from IHS_SurpriseAnalyst_cleaned  where dates = '2009-07-31' order by QSA_Composite

 --QSA_Efficiency factor(surpirse analyst compisite rank)
 select dates,dscode,name,QSA_Efficiency,lead_2day_return,
percentile_cont(0.2) within group (order by QSA_Efficiency) over (partition by dates) as a,
percentile_cont(0.8) within group (order by QSA_Efficiency) over (partition by dates) as b,
 (QSA_Efficiency-AVG(QSA_Efficiency) over (partition by dates )) as mean_QSA_Efficiency,
stdev(QSA_Efficiency) over (partition by dates ) as std_QSA_Efficiency
into #b1
 from IHS_SurpriseAnalyst_cleaned


 select * from #a11 order by QSA_Efficiency,dates,dscode
 delete from #b1 where  std_QSA_Efficiency = 0
 select a.*, mean_QSA_Efficiency/std_QSA_Efficiency as zscore_QSA_Efficiency into IHS_SurpriseAnalyst_QSA_Efficiency
 from #b1 a 
 order by dates,dscode

 select * from IHS_SurpriseAnalyst_QSA_Efficiency order by QSA_Efficiency,dates,dscode

  --QSA_EstExpect factor(surpirse analyst compisite rank)
 select dates,dscode,name,QSA_EstExpect,lead_2day_return,
percentile_cont(0.2) within group (order by QSA_EstExpect) over (partition by dates) as a,
percentile_cont(0.8) within group (order by QSA_EstExpect) over (partition by dates) as b,
 (QSA_EstExpect-AVG(QSA_EstExpect) over (partition by dates )) as mean_QSA_EstExpect,
stdev(QSA_EstExpect) over (partition by dates ) as std_QSA_EstExpect
into #c1
 from IHS_SurpriseAnalyst_cleaned



 delete from #c1 where  std_QSA_EstExpect = 0
 select a.*, mean_QSA_EstExpect/std_QSA_EstExpect as zscore_QSA_EstExpect into IHS_SurpriseAnalyst_QSA_EstExpect
 from #c1 a 
 order by dates,dscode

 select * from IHS_SurpriseAnalyst_QSA_Efficiency order by QSA_Efficiency,dates,dscode

   --QSA_Percent factor(surpirse analyst compisite rank)
 select dates,dscode,name,QSA_Percent,lead_2day_return,
percentile_cont(0.2) within group (order by QSA_Percent) over (partition by dates) as a,
percentile_cont(0.8) within group (order by QSA_Percent) over (partition by dates) as b,
 (QSA_Percent-AVG(QSA_Percent) over (partition by dates )) as mean_QSA_Percent,
stdev(QSA_Percent) over (partition by dates ) as std_QSA_Percent
into #d1
 from IHS_SurpriseAnalyst_cleaned


 delete from #d1 where  std_QSA_Percent = 0
 select a.*, mean_QSA_Percent/std_QSA_Percent as zscore_QSA_Percent into IHS_SurpriseAnalyst_QSA_Percent
 from #d1 a 
 order by dates,dscode


 --QSA_SurpSN factor(surpirse analyst compisite rank)
 select dates,dscode,name,QSA_SurpSN,lead_2day_return,
percentile_cont(0.2) within group (order by QSA_SurpSN) over (partition by dates) as a,
percentile_cont(0.8) within group (order by QSA_SurpSN) over (partition by dates) as b,
 (QSA_SurpSN-AVG(QSA_SurpSN) over (partition by dates )) as mean_QSA_SurpSN,
stdev(QSA_SurpSN) over (partition by dates ) as std_QSA_SurpSN
into #e1
 from IHS_SurpriseAnalyst_cleaned


 drop table #e1

 delete from #e1 where  std_QSA_SurpSN = 0
 select a.*, mean_QSA_SurpSN/std_QSA_SurpSN as zscore_QSA_SurpSN into IHS_SurpriseAnalyst_QSA_SurpSN
 from #e1 a 
 order by dates,dscode

 --check correlation to make sure the desc and asc order
 select * from IHS_SurpriseAnalyst_cleaned where dscode = '992816' order by dates