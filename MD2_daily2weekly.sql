/***---generate weekly data by using daily MD2_timeSeries data---***/
--using ROW_NUMBER function
--notice that there're several columns like high,low,return...which need to be calculated specifically(into table2 and table3, then combine)

/---*******Part 1****************---/
--this part is used for generating close related columns (data except volume/return and open)
select row_number() over (partition by dscode order by dates) as [period], 
dates,
datepart(yyyy,dates) as year,
datepart(mm,dates) as month,
datepart(WK,dates) as week,
datepart(iso_week,dates) as iso_weekNumber,
[closeprice], [dscode],[CapitalAdjustmentFactor],[CashAdjustmentFactor],
[CompanyMarketValueUSDBloomberg],[DatastreamDY],[DatastreamWC05350],[DatastreamWC05905],[DividendYield],[FreeFloatFraction],[FTSEICBCode],
[FTSEICBIndustry],[FTSEICBSector],[FTSEICBSubsector],[FTSEICBSupersector],[IsActiveForJH],[IsAlive],[IsPrimaryListingOfSecurity],
[IsPrimarySecurityOfCompany],[IsRestrictedByGSA],[IsSuspendedByExchange],[LastAsk],[LastBid],[MTFTradedValueUSD],[PaddedClose],
[PaddedMarketValue],[PaddedMarketValueUSD],[PaddedUnadjustedClose],[PaddedUSDRate],[RoundLotSize],[SpreadBPS25thPercentile],
[SpreadBPS50thPercentile],[SpreadBPS75thPercentile],[TradedValueUSD],[UnadjustedClose],[UnadjustedSharesOutstanding],[ReturnIndex]
--into tb_1_MD2
from MD2_daily


where dates in( 
--get last pricedate of each period
select max(dates) as a from MD2_daily where datepart(iso_week,dates) < 53
group by datepart(yyyy,dates) ,datepart(iso_week,dates)
)
order by dates,dscode


select *from tb_1_MD2 order by dscode,year, month, week

/---*******Part 2****************---/
---this part is used for generating open related columns
select  
dates,dscode,
datepart(yyyy,dates) as year,
datepart(mm,dates) as month,
datepart(WK,dates) as week,
[openprice],[UnadjustedOpen]
into md2_openRelated
from MD2_daily
--get first pricedate of each period
where dates in (
select min(dates) as a from MD2_daily where datepart(WK,dates) < 53
group by  datepart(yyyy,dates) ,datepart(WK,dates)
)

select * from tb_2_MD2_temp order by dscode,year, month, week
--drop dates column in order to join with other table
alter table tb_2_MD2_temp
drop column dates



/---*******Part 3****************---/

--put return related columns and volume related columns into table_2
--generate #temp table which contains year,month and week number
select datepart(WK,dates) as week_number,datepart(yyyy,dates) as year_number,
datepart(mm,dates) as month_number, MD2_daily.* into temp_weekly from md2_daily

--this part is used for generating return/sum related columns into #temp2
select  year_number, month_number, week_number, sum(a.returns) as sum_returns, sum(a.volume) as sum_volume,
sum(UnadjustedMTFVolume) as sum_UnadjustedMTFVolume, sum(PaddedVolume) as sum_PaddedVolume, sum(MTFVolume) as sum_MTFVolume, dscode
into tb_3_MD2
from temp_weekly a
group by dscode,year_number, month_number, week_number
order by dscode,year_number, month_number, week_number

select * from tb_3_MD2 order by dscode,year_number, month_number, week_number



/---*******Part 4****************---/
--join the above three tables
select a.*, b.openprice, b.UnadjustedOpen,c.sum_returns,c.sum_volume,c.sum_UnadjustedMTFVolume,c.sum_PaddedVolume,c.sum_MTFVolume into MD2_timeSeries_weekly
from tb_1_MD2 a 
join tb_2_MD2_temp b on a.dscode = b.dscode and a.year = b.year and a.month = b.month and a.week = b.week
join tb_3_MD2 c on a.dscode = c.dscode and a.year = c.year_number and a.month = c.month_number and a.week = c.week_number
select* from MD2_timeSeries_weekly order by dscode,year, month, week










/---*******New Version for MD2_timeseries****************---/

/---*******Part 1****************---/
---this part is used for generating close related columns (data except volume/return and open)
--- step1: generate new column(iso_weekNumber) and column(row_number) into new table
select datepart(WK,dates) as week_number,datepart(yyyy,dates) as year_number,
datepart(weekday,[dates]) as DayPerWeek, datepart(iso_week,[dates]) as iso_weekNumber ,MD2_daily.* into #md2_1 from md2_daily 
order by dates,dscode
select * from #md2_1 order by dates,dscode
select * from #md2_3 where  dates >'2008-12-24' and dates <'2009-01-10' order by dates



--step2: combine period(1-5) with md2_1(in order to use group by method to delete extra days in the week when a stock was delsited),WHICH means get weekly data
select #md2_1.*,row_number() over (partition by dscode,iso_weekNumber,year_number order by dates desc) as [period]  into #md2_2 from #md2_1 
select #md2_2.* into #md2_3 from #md2_2 where [period]='1' order by dates,dscode 

select * from #md2_3 order by dates,dscode,iso_weekNumber 
select * from #md2_3 where iso_weekNumber = '53' order by dates




/---*******Part 2****************---/
---this part is used for generating open related columns
---ignore the openprice
---select #md2_1.Dates,#md2_1.Dscode, #md2_1.year_number, #md2_1.month_number,#md2_1.iso_weekNumber,#md2_1.OpenPrice,
---#md2_1.UnadjustedOpen, row_number() over (partition by dscode,iso_weekNumber,year_number order by dates ) as [period]   into #md2temp from md2_1  ORDER BY dates,dscode
---select *  into MD2_3 from #md2temp where [period]='1' order by dates,dscode


/---*******Part 3****************---/
--deal with return related columns and volume related columns
--this part is used for generating return/sum related columns into #temp2
select  year_number,  iso_weekNumber, sum(a.returns) as sum_returns, sum(a.volume) as sum_volume,
sum(UnadjustedMTFVolume) as sum_UnadjustedMTFVolume, sum(PaddedVolume) as sum_PaddedVolume, sum(MTFVolume) as sum_MTFVolume, dscode
into #MD2_4
from #MD2_1 a
group by dscode,year_number,  a.iso_weekNumber
order by dscode,year_number,  a.iso_weekNumber
select * from #md2_4 where iso_weekNumber = '53' order by year_number,  iso_weekNumber


/---*******Part 4****************---/
--join the above two tables( #md_3 is close price related table and #md_4 is sum(return) and sum(volume) talbe)
select a.*,b.sum_returns,b.sum_volume,b.sum_UnadjustedMTFVolume,b.sum_PaddedVolume,b.sum_MTFVolume INTO MD2_timeseries_weekly_new
from #MD2_3 a 
join #MD2_4 b on a.dscode = b.dscode and a.year_number = b.year_number and  a.iso_weekNumber = b.iso_weekNumber
select * from MD2_timeseries_weekly_new order by dates,dscode,year_number,iso_weekNumber
select * from MD2_timeseries_weekly_new where  dates >'2008-12-20' and dates <'2009-01-10' order by dates , year_number,  iso_weekNumber

select * from MD2_timeseries_weekly_new where iso_weekNumber = '53' order by dates,year_number,  iso_weekNumber




----generate YearWeek column in the final daily table
--first write the yearweek function, which will transfer '2018-10-17' into '201842'
CREATE FUNCTION [intra\eliu].yearweek(@date date)
RETURNS INT
as
 begin
    set @date = dateadd(dd,-datepart(dw,@date)+1, @date)

    return datepart(year,@date)*100 + datepart(week,@date)
 end
go

 select [MD2_timeseries_weekly_new].*, [intra\eliu].yearweek(dates) as year_week into [MD2_timeseries_weekly] from [MD2_timeseries_weekly_new]

 select * from [MD2_timeseries_weekly] order by year_week,dscode



