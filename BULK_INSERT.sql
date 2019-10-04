/****** Script for SelectTopNRows command from SSMS  ******/


BULK INSERT LIANGPI.[intra\eliu].[IBES_Summary_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_Summary_Estimates_updates.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);



BULK INSERT LIANGPI.[intra\eliu].[IBES_Summary_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_Summary_Estimates_snapshot.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[IBES_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_Estimates_snapshot.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[IBES_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_Estimates_updates.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[IBES_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_Estimates_snapshot_part2.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[IBES_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_Estimates_updates_part2.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[IBES_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_Estimates_snapshot_part3.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[IBES_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_Estimates_updates_part3.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);



---IBES_KPI Data
BULK INSERT LIANGPI.[intra\eliu].[IBES_KPI_Actuals] FROM '\\lospliangpisql1\ELIU\\IBES_KPI_actuals_updates.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);



BULK INSERT LIANGPI.[intra\eliu].[IBES_KPI_Actuals] FROM '\\lospliangpisql1\ELIU\\IBES_KPI_actuals_snapshot.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


BULK INSERT LIANGPI.[intra\eliu].[IBES_KPI_Summary_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_KPI_Summary_Estimates_updates.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);



BULK INSERT LIANGPI.[intra\eliu].[IBES_KPI_Summary_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_KPI_Summary_Estimates_snapshot.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


BULK INSERT LIANGPI.[intra\eliu].[IBES_KPI_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_KPI_Estimates_updates.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);



BULK INSERT LIANGPI.[intra\eliu].[IBES_KPI_Estimates] FROM '\\lospliangpisql1\ELIU\\IBES_KPI_Estimates_snapshot.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);








--- smart holdings daily data---
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2000_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2001_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2002_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2003_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);





BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2004_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2005_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2006_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2007_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2008_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);




BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2009_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2010_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2011_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2012_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2013_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2014_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2015_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2016_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2017_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
BULK INSERT LIANGPI.[intra\eliu].[StarMine_Smart_Holdings_Daily] FROM '\\lospliangpisql1\ELIU\\StarMine_Smart_Holdings_Daily_2018_20180806.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);



---Starmine smartholdings_ARM_daily
BULK INSERT LIANGPI.[intra\eliu].[StarMine_ARM_Daily] FROM '\\lospliangpisql1\ELIU\\Starmine_ARM_daily.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


--alter table [StarMine_ARM_Daily_new]
--alter column ARM_ex_recommendations float
--truncate table [StarMine_ARM_Daily_new]
--exec sp_rename 'StarMine_ARM_Daily_new', 'StarMine_ARM_Daily'



/*******create TRMI table and map table********/
BULK INSERT LIANGPI.[intra\eliu].[TRMI] FROM '\\lospliangpisql1\ELIU\\TRMI.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[TRMI_Map] FROM '\\lospliangpisql1\ELIU\\TRMI_Map.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


/*******create extended IBES_Healthcare industry table********/
BULK INSERT LIANGPI.[intra\eliu].[IBES_Healthcare_extend] FROM '\\lospliangpisql1\ELIU\\IBES_Healthcare_extended.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[IBES_Healthcare_new] FROM '\\lospliangpisql1\ELIU\\IBES_Healthcare_new2.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[IBES_Healthcare_new3] FROM '\\lospliangpisql1\ELIU\\IBES_Healthcare_new3.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


select returns from MD2_timeseries where dscode = '29353C' and dates >'2013-02-01' and dates<'2013-02-18'

BULK INSERT LIANGPI.[intra\eliu].[2iQ_daily] FROM '\\lospliangpisql1\ELIU\\combined_2iQ_daily.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

BULK INSERT LIANGPI.[intra\eliu].[2iQ_Map_daily] FROM '\\lospliangpisql1\ELIU\\companySecurity-enhanced.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);
alter table LIANGPI.[intra\eliu].[2iQ_Map_daily]
alter column exchCode varchar(100)

truncate table [2iQ_Map_daily]


BULK INSERT LIANGPI.[intra\eliu].[temp2iQ_2017_2018] FROM '\\lospliangpisql1\ELIU\\2017_2018.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


alter table [map_2iq_daily2]
alter column exchgdesc varchar(100)


truncate table [map_2iq_daily]
 select * from [map_2iq_daily]



BULK INSERT LIANGPI.[intra\eliu].[Map_2iQ_daily] FROM '\\lospliangpisql1\ELIU\\companySecurity-enhanced.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);




----some columns have "," within each cell.... so I have to use FIELDTERMINATOR = '|'
BULK INSERT LIANGPI.[intra\eliu].[2iQ_Raw_daily] FROM '\\lospliangpisql1\ELIU\\combined_2iQ_raw_daily.csv'
   WITH (
      FIELDTERMINATOR = '|',
      ROWTERMINATOR = '\n'
);

select * from [2iQ_Raw_daily] where exchange = 'NYSE/NASDAQ/AMEX (USA)' order by companyname,timestamp


alter table [2iQ_Raw_daily]
alter column holdings  float

select * from [2iQ_Raw_daily]

truncate table [2iQ_Raw_daily]


BULK INSERT LIANGPI.[intra\eliu].[CAM1] FROM '\\lospliangpisql1\ELIU\\CAM1_History_2005_201807_new.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);

select * from CAM1 order by dates

BULK INSERT LIANGPI.[intra\eliu].[ClosingBell1] FROM '\\lospliangpisql1\ELIU\\ClosingBell_Ratings_history_2014_201804_new.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


BULK INSERT LIANGPI.[intra\eliu].[Digital_Revenue_Signal] FROM '\\lospliangpisql1\ELIU\\Digital_Revenue_Signal_history_2012_201807_new.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


BULK INSERT LIANGPI.[intra\eliu].[TM2] FROM '\\lospliangpisql1\ELIU\\TM1_History_2000_201805_new.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);


BULK INSERT LIANGPI.[intra\eliu].[tress1] FROM '\\lospliangpisql1\ELIU\\TRESS_history_2010_201801_new.csv'
   WITH (
      FIELDTERMINATOR = ',',
      ROWTERMINATOR = '\n'
);