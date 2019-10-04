/******Quality size investing*****/
select dates,dscode,name,GPA,ROE,ROA,CFITA,Min1YGrossMargin,TTMAccu,
[1YChgGPA],YOYChgROEArt,YOYChgAA,YOYChgROA,ChgCF,
Beta,DFL,Ohlsonscore,DivGP
into #t1 
from  [LIANGPI].[dbo].[IHS_Markit_Universe]

drop table profitability



/******profitability part*****/

select dates,dscode,name,GPA,ROE,ROA,CFITA,Min1YGrossMargin,TTMAccu,
(GPA - AVG(GPA) over (partition by dates))/ (stdev(GPA) over (partition by dates)) * -1 as zscore_GPA,
(ROE - AVG(ROE) over (partition by dates))/ (stdev(ROE) over (partition by dates)) * -1 as zscore_ROE,
(ROA - AVG(ROA) over (partition by dates))/ (stdev(ROA) over (partition by dates)) * -1 as zscore_ROA,
(CFITA - AVG(CFITA) over (partition by dates))/ (stdev(CFITA) over (partition by dates)) * -1 as zscore_CFITA,
(Min1YGrossMargin - AVG(Min1YGrossMargin) over (partition by dates))/ (stdev(Min1YGrossMargin) 
over (partition by dates)) * -1 as zscore_Min1YGrossMargin,
(TTMAccu - AVG(TTMAccu) over (partition by dates))/ (stdev(TTMAccu) over (partition by dates)) * -1 as zscore_TTMAccu
INTO #t2
from #t1

drop table profitability

SELECT a.*, (zscore_GPA+zscore_ROE+zscore_ROA+zscore_CFITA+zscore_Min1YGrossMargin+zscore_TTMAccu) as zscore_profitability2
into #t33 
from #t2 a 

select a.*, 
(zscore_profitability2 - AVG(zscore_profitability2) over (partition by dates))/ (stdev(zscore_profitability2) 
over (partition by dates))  as zscore_profitability
into #t3 from #t33 a 

--join with return table
select a.*,b.lead_2day_return
into profitability
from #t3 a 
join MD2_lead_2days_return b
on a.dates = b.dates and a.dscode = b.dscode
where zscore_profitability is not null

/******growth part*****/
select dates,dscode,name,[1YChgGPA],YOYChgROEArt,YOYChgAA,YOYChgROA,ChgCF,
([1YChgGPA] - AVG([1YChgGPA]) over (partition by dates))/ (stdev([1YChgGPA]) over (partition by dates)) * -1 as zscore_1YChgGPA,
(YOYChgROEArt - AVG(YOYChgROEArt) over (partition by dates))/ (stdev(YOYChgROEArt) over (partition by dates)) * -1 as zscore_YOYChgROEArt,
(YOYChgAA - AVG(YOYChgAA) over (partition by dates))/ (stdev(YOYChgAA) over (partition by dates)) * -1 as zscore_YOYChgAA,
(YOYChgROA - AVG(YOYChgROA) over (partition by dates))/ (stdev(YOYChgROA) over (partition by dates)) * -1 as zscore_YOYChgROA,
(ChgCF - AVG(ChgCF) over (partition by dates))/ (stdev(ChgCF) over (partition by dates)) * -1 as zscore_ChgCF
into #t4
from #t1

select a.*,
(zscore_1YChgGPA+zscore_YOYChgROEArt+zscore_YOYChgAA+zscore_YOYChgROA+zscore_ChgCF) as zscore_growth2
into #t5
from #t4 a 

select a.*, 
(zscore_growth2 - AVG(zscore_growth2) over (partition by dates))/ (stdev(zscore_growth2) 
over (partition by dates))  as zscore_growth
into #t55 from #t5 a 

--join with return table
drop table growth
select a.*,b.lead_2day_return
into growth
from #t55 a 
join MD2_lead_2days_return b
on a.dates = b.dates and a.dscode = b.dscode
where zscore_growth is not null


/******safety part*****/
select dates,dscode,name,Beta,DFL,Ohlsonscore,
(Beta - AVG(Beta) over (partition by dates))/ (stdev(Beta) over (partition by dates)) * -1 as zscore_Beta,
(DFL - AVG(DFL) over (partition by dates))/ (stdev(DFL) over (partition by dates))  as zscore_DFL,
(Ohlsonscore - AVG(Ohlsonscore) over (partition by dates))/ (stdev(Ohlsonscore) over (partition by dates))  as zscore_Ohlsonscore
into #t6
from #t1

drop table safety
select a.*,(zscore_Beta+zscore_DFL+zscore_Ohlsonscore) as zscore_safety2
into #t7
from #t6 a 

select a.*, 
(zscore_safety2 - AVG(zscore_safety2) over (partition by dates))/ (stdev(zscore_safety2) 
over (partition by dates))  as zscore_safety
into #t77 from #t7 a 

--join with return table
select a.*,b.lead_2day_return
into safety
from #t77 a 
join MD2_lead_2days_return b
on a.dates = b.dates and a.dscode = b.dscode
where zscore_safety is not null

/******pay out*****/
drop table payout 
select dates,dscode,name,DivGP,
(DivGP - AVG(DivGP) over (partition by dates))/ (stdev(DivGP) over (partition by dates)) * -1 as zscore_payout
into #t8 
from #t1

--join with return table
select a.*,b.lead_2day_return
into payout
from #t8 a 
join MD2_lead_2days_return b
on a.dates = b.dates and a.dscode = b.dscode
where zscore_payout is not null


--join four quality characteristic tables and get the final zscore_quality
select a.dates,a.dscode,a.zscore_profitability,a.lead_2day_return, b.zscore_growth,c.zscore_safety,d.zscore_payout
into #t9
from profitability a 
join growth b on a.dates = b.dates and a.dscode = b.dscode
join safety c on a.dates = c.dates and a.dscode = c.dscode
join payout d on a.dates = d.dates and a.dscode = d.dscode

drop table Quality

select a.*, (zscore_profitability + zscore_growth + zscore_safety + zscore_payout) as zscore
into #t10
from #t9 a 

select a.*,
(zscore - AVG(zscore) over (partition by dates))/ (stdev(zscore) over (partition by dates))  as zscore_quality
into Quality
from #t10 a 


select * from Quality  where dates = '2018-02-08' order by dates,zscore_Quality

---join with  [LIANGPI].[dbo].[RESIDUAL_RETURNS_AXIOMA_1000] and use FACTORS_4_ind residual return
select * into #x1
from  [LIANGPI].[dbo].[RESIDUAL_RETURNS_AXIOMA_1000]  where strategy = 'FACTORS_4_ind'

drop table #lead_2day_return
select lead(DAILY_RETURN,2) over (partition by dscode order by ENTRY_DATE) lead_2day_residual_return,
ENTRY_DATE,dscode into #lead_2day_return from #x1


drop table Quality_Residual

select a.*,b.lead_2day_residual_return 
into Quality_Residual
from Quality a
join #lead_2day_return b
on a.dates = b.ENTRY_DATE and a.dscode = b.dscode

select top(10000)* from #q3  order by dates,zscore_Quality


---Quality Minus Junk(QMJ)
--get Size factor(NLMKtCap) from IHS talbe
select dates,dscode,NLMKtCap into #q1 FROM IHS_Markit_Universe

--generate mean in order to separate the universe
delete from #q1 where NLMktCap is null
drop table #q3
select a.*, percentile_cont(0.5) within group( order by NLMktCap) over (partition by dates) as mean_NLMktCap
into #q2
from #q1 a
order by dates,dscode

--join with quality table
select a.*,b.NLMktCap into #q3
from Quality_Residual a 
join #q1 b
on a.dates = b.dates and a.dscode = b.dscode

--select * from #q3 where NLMktCap is null or mean_NLMktCap is null order by dates,NLMktCap
select  * from #q6 order by dates,NLMktCap

--generate top 30% quality and bottom 30% quality columns 
select a.*,percentile_cont(0.3) within group (order by zscore_quality) over (partition by dates) as bottom_quality,
percentile_cont(0.7) within group (order by zscore_quality) over (partition by dates) as top_quality
into #q4
from #q3 a

select a.*, percentile_cont(0.3) within group( order by NLMktCap) over (partition by dates) as small_NLMktCap,
percentile_cont(0.7) within group( order by NLMktCap) over (partition by dates) as large_NLMktCap
into #q5
from #q4 a

--generate small quality and large quality (long side)
select dates,dscode,lead_2day_return, lead_2day_residual_return,zscore_quality,NLMktCap,bottom_quality,top_quality,
small_NLMktCap, large_NLMktCap into #q6
from #q5
where NLMktCap >= large_NLMktCap or NLMktCap<= small_NLMktCap
order by dates,NLMktCap

--QMJ LongSide
select * into #q7 from #q6 where zscore_quality >= top_quality
select  * from #q9 order by dates,NLMktCap

--same for the shortside
select * into #q8 from #q6 where zscore_quality <= bottom_quality

--union long and short side, then re-standardize the quality factor(QMJ)
select * into #q9
from #q7 
union all
select * from #q8

select a.*,
(zscore_quality - AVG(zscore_quality) over (partition by dates))/ 
(stdev(zscore_quality) over (partition by dates))  as QMJ
into QMJ
from #q9 a 


--QMJ , not incorporate size effect
SELECT * from #q5  order by dates,dscode


select dates,dscode,lead_2day_return, lead_2day_residual_return,zscore_quality,bottom_quality,top_quality
into #q10
from #q5
where zscore_quality >= top_quality or zscore_quality <= bottom_quality

select a.*, 
(zscore_quality - AVG(zscore_quality) over (partition by dates))/ 
(stdev(zscore_quality) over (partition by dates))  as QMJ
into QMJ_No_Size
from #q10 a 

select * from QMJ  order by dates,QMJ

--generate positions for QMJ
--generate strategy name and entry time column
ALTER table QMJ
add STRATEGY varchar(50) not null default 'Quality_Minus_Junk'

alter table QMJ
add ENTRY_TIME datetime

--generate POSITION column
drop table  #q12

select a.*, QMJ*(-1)/SUM(QMJ) over (partition by dates) as POSITION into #q12
from QMJ a
where QMJ <= 0

UNION ALL

select a.*,QMJ/SUM(QMJ) over (partition by dates) as POSITION 
from QMJ a
where QMJ > 0
order by dates,dscode


select * from #q12 order by dates,dscode

--generate position table and insert into dbo.POSITION_Research
DROP TABLE #q13
select dbo.nextTD(dates) as ENTRY_DATE, ENTRY_TIME, STRATEGY, Dscode, POSITION into #q13 from #q12 order by ENTRY_DATE,dscode

insert into [LIANGPI].[dbo].[POSITIONS_Research]
select * from #q13
order by ENTRY_DATE,dscode


select * from [LIANGPI].[dbo].[POSITIONS_Research] where strategy ='Quality_Minus_Junk' order by ENTRY_DATE, dscode

delete from [LIANGPI].[dbo].[POSITIONS_Research] where strategy ='Quality_Minus_Junk'

select * from QMJ_Regression order by dates,dscode

select a.*,
(NLMktCap - AVG(NLMktCap) over (partition by dates))/ 
(stdev(NLMktCap) over (partition by dates))  as zscore_NLMktCap
into QMJ_Regression
from QMJ a 



/**************Redo the regression QMJ part***********/
--select * from #q3 where NLMktCap is null or mean_NLMktCap is null order by dates,NLMktCap
drop table QMJ2

select dates,dscode,zscore_quality,lead_2day_return,lead_2day_residual_return,NLMktCap,
(NLMktCap - AVG(NLMktCap) over (partition by dates))/ 
(stdev(NLMktCap) over (partition by dates))  as zscore_NLMktCap
into QMJ
from #q3 a 

select * from QMJ2 where dates = '2006-12-01' order by zscore_NLMktCap