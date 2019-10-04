/****** Script for calculate turnover for IHS factors  ******/
SELECT [Dates]
      ,[Dscode]
      ,[Ticker]
      ,[Cusip]
      ,[Name],PC_Ratio,ATMCallVol,ATMPutVol,VolDiff_PC,OTMPTC,AG,ImpVol,IMR
  into #temp1
  FROM [LIANGPI].[dbo].[IHS_Markit_Industry_Neutral2] 
  ORDER BY dates,dscode


  --generate long table which contains all these 8 factors in one 'strategy' column
  create table ihs_long(
  Dates date,
  Dscode varchar(100),
  alphascore float,
  strategy varchar(100))
  insert into  ihs_long 
  select dates,dscode, pc_ratio, 'PC_ratio' as strategy
  from #temp1
  union all
  select dates,dscode, ATMCallVol, 'ATMCallVol' as strategy
  from #temp1
  union all
  select dates,dscode, ATMPutVol, 'ATMPutVol' as strategy
  from #temp1
  union all
  select dates,dscode, VolDiff_PC, 'VolDiff_PC' as strategy
  from #temp1
  union all
    select dates,dscode, OTMPTC, 'OTMPTC' as strategy
	from #temp1
  union all
    select dates,dscode, AG, 'AG' as strategy
	from #temp1
  union all
    select dates,dscode, ImpVol, 'ImpVol' as strategy
	from #temp1
  union all
    select dates,dscode, IMR, 'IMR' as strategy
  from #temp1


  select * from ihs_long order by dates,dscode

  --generate new_alphascore which make sure sum of long side equals to 1 and shortside is -1 for each day
  select dates,dscode,strategy, alphascore, alphascore/sum(alphascore) over (partition by dates,strategy) as new_alphascore
  into #temp2
  from ihs_long
  where alphascore > '0'

  union all

  select dates,dscode,strategy, alphascore, alphascore/sum(alphascore) over (partition by dates,strategy)*-1 as new_alphascore
  from ihs_long
  where alphascore < '0'
  order by dates,dscode



  /****TOO slow to run the combine table*****/
  --get the previous day and join two tables
 select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #temp4
 from #temp2 a1 full outer join #temp2 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore

 /***---So I perfer run each factor separately****/
 --PC_ratio
  select * into #PC_ratio1 from #temp2  where strategy = 'PC_ratio'  order by dates,strategy

  select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #p1
 from #PC_ratio1 a1 full outer join #PC_ratio1 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore
 select * from #p1 order by dates,dscode
  --ATMCallVol
  select * into #ATMCallVol1 from #temp2  where strategy = 'ATMCallVol'  order by dates,strategy
  select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #p2
 from #ATMCallVol1 a1 full outer join #ATMCallVol1 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore

 select * from #p2  order by dates, new_alphascore

 --ATMPutVol
  select * into #ATMPutVol1 from #temp2  where strategy = 'ATMPutVol'  order by dates,strategy
  select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #p3
 from #ATMPutVol1 a1 full outer join #ATMPutVol1 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore

  --VolDiff_PC
  select * into #VolDiff_PC1 from #temp2  where strategy = 'VolDiff_PC'  order by dates,strategy
  select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #p4
 from #VolDiff_PC1 a1 full outer join #VolDiff_PC1 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore


   --OTMPTC
  select * into #OTMPTC1 from #temp2  where strategy = 'OTMPTC'  order by dates,strategy
  select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #p5
 from #OTMPTC1 a1 full outer join #OTMPTC1 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore

    --AG
  select * into #AG1 from #temp2  where strategy = 'AG'  order by dates,strategy
  select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #p6
 from #AG1 a1 full outer join #AG1 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore


     --ImpVol
  select * into #ImpVol1 from #temp2  where strategy = 'ImpVol'  order by dates,strategy
  select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #p7
 from #ImpVol1 a1 full outer join #ImpVol1 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore

     --IMR
  select * into #IMR1 from #temp2  where strategy = 'IMR'  order by dates,strategy
  select a1.*,a2.new_alphascore as prev_new_alphascore,a2.dates as prev_date  into #p8
 from #IMR1 a1 full outer join #IMR1 a2 
 on a2.dates = dbo.prevTD(a1.dates) and a1.dscode = a2.dscode
 order by dates, new_alphascore


    --replace null value with 0 for current position column and previous return column)
 select isnull(new_alphascore,0) current_position, isnull(prev_new_alphascore,0) previous_position,dates,dscode,prev_date,strategy
 into #IMR
 from #p8
 order by dates

  ---check if long/short side for each day sums to 0
 select * from #turnover_PC_ratio  where dates = '2006-01-30' order by current_position
  --calculate the turnover for each trading day
 select (sum(abs(current_position- previous_position))) as turnover_equalWtd, dates  
 from #IMR
 group by dates
 order by dates
