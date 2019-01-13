# XetraMetrics
This application runs three separate analyses against the public XETRA
dataset from Deutsche Bank (publicly available at s3://deutsche-boerse-xetra-pds on AWS
or at <link>https://drive.google.com/open?id=1QmcwvzyWp7RBwPSagQoIbbZqe_6E9jaC</link>
on Google Drive):
1. Daily biggest winners: one record per date, sorted by date,
representing the stock/fund with the largest gain for the given date
2. Daily biggest volumes: one record per date, sorted by date,
representing the stock/fund with the highest trade volume for the given date
3. Overall implied volume: one record per stock/fund,
sorted in descending order by the implied volume amount for each stock

The task was to implement several SQL queries from
https://github.com/Deutsche-Boerse/dbg-pds/tree/master/examples/sql (located in the
resources directory),
each of which handles one of the above analyses, using the Apache Spark API.
For the first 2 analyses, it compares two methodologies: a SQL-like approach that
consists of joining multiple datasets, modeled after the related example queries, and
sessionization. The third analysis is not eligible for this comparison, as the related
example query does not employ any join logic, so the application only runs a
sessionization program for it.

I wanted to run this comparison of sessionization and joining logic to demonstrate
different approaches to analyzing this data, as well as to determine which approach
might be "better."__*__

*<sub>\*I say "better" because there are so many constraints that don't apply to this
small app running on a local machine - but that would matter in a production system -
that I am hesitant to make a real conclusion. This was simply an experiment and
demonstration.</sub>*


## Downloading the data
The data is not included in this repo due to size constraints. After downloading the data
from either S3 or Google Drive, you may have to decompress the file.
~~~
tar -xvf data-set.tar
~~~

Once this is done, the resultant file structure should look something like this (possibly
with different dates):
~~~
├── data-set
│   ├── 2018-02-01
│   ├── 2018-02-02
│   ├── 2018-02-05
│   ├── 2018-02-06
~~~

Copy the directory `data-set/` into the project at `src/main/resources/`.

## Compiling and packaging the application
~~~
mvn clean package
~~~

## Running the application
~~~
spark2-submit \
--class com.acorns.techtest.XetraMetrics`
~~~

## Verifying the output
First, a 10 record sample of the XETRA source data will be printed to stdout, along with the total row count of the source data.

Next, for each analysis, several items will be printed to stdout:
- A sample of the output (10 rows)
- A row count for the entire data set
- Run time in milliseconds

The above information will be printed to stdout for each methodology, as will a diff of the two output datasets.
This is to inform the user which approach might be better: as long as the output datasets are the same,
then performance is the only remaining consideration. 

## Sample output
~~~
Found 1415917 source records.
--------SOURCE SAMPLE---------
TradeActivity(AT0000A0E9W5,SANT,S+T AG (Z.REG.MK.Z.)O.N.,Common stock,EUR,2504159,2018-02-06 00:00:00,09:00,20.04,20.04,19.91,19.95,3314,16)
TradeActivity(AT00000FACC2,1FC,FACC AG INH.AKT.,Common stock,EUR,2504163,2018-02-06 00:00:00,09:00,16.5,16.5,16.5,16.5,250,2)
TradeActivity(AT0000743059,OMV,OMV AG,Common stock,EUR,2504175,2018-02-06 00:00:00,09:00,48.8,48.8,48.8,48.8,164,5)
TradeActivity(AT0000937503,VAS,VOESTALPINE AG,Common stock,EUR,2504189,2018-02-06 00:00:00,09:00,49.63,49.64,49.63,49.63,200,2)
TradeActivity(AT0000969985,AUS,AT+S AUSTR.T.+SYSTEMT.,Common stock,EUR,2504191,2018-02-06 00:00:00,09:00,21.6,21.6,21.4,21.6,728,4)
TradeActivity(BE0974293251,1NBA,ANHEUSER-BUSCH INBEV,Common stock,EUR,2504195,2018-02-06 00:00:00,09:00,86.76,86.76,86.76,86.76,19,1)
TradeActivity(CH0038389992,BBZA,BB BIOTECH NAM.   SF 0;20,Common stock,EUR,2504244,2018-02-06 00:00:00,09:00,55.85,55.85,55.85,55.85,100,1)
TradeActivity(DE000A0X8994,DEL2,ETFS DAX DLY 2X LG GO DZ,ETF,EUR,2504256,2018-02-06 00:00:00,09:00,301.5,301.5,301.5,301.5,20,1)
TradeActivity(DE000A0X9AA8,DES2,ETFS DAX DLY2XSH.GO UC.DZ,ETF,EUR,2504257,2018-02-06 00:00:00,09:00,4.794,4.794,4.794,4.794,300,1)
TradeActivity(DE000ETFL011,EL4A,DK DAX,ETF,EUR,2504258,2018-02-06 00:00:00,09:00,114.08,114.08,114.08,114.08,10,1)
------------------------------


/*****************************
     DAILY BIGGEST WINNERS
*****************************/

Found 20 daily biggest winners by sessionizing in 3002 ms.
-----SESSIONIZING SAMPLE------
DailyBiggestWinner(2018-02-01 00:00:00,2505107,BEATE UHSE AG,0.44444444444444453)
DailyBiggestWinner(2018-02-02 00:00:00,2506525,MATTEL INC.          DL 1,0.1885964912280702)
DailyBiggestWinner(2018-02-05 00:00:00,2505364,AIR BERLIN PLC    EO -;25,0.10803324099722994)
DailyBiggestWinner(2018-02-06 00:00:00,2505100,PVA TEPLA AG O.N.,0.1869918699186991)
DailyBiggestWinner(2018-02-07 00:00:00,2505025,ARCANDOR AG O.N.,0.25)
DailyBiggestWinner(2018-02-08 00:00:00,2506580,TWITTER INC.   DL-;000005,0.18804597701149425)
DailyBiggestWinner(2018-02-09 00:00:00,2505025,ARCANDOR AG O.N.,0.25)
DailyBiggestWinner(2018-02-12 00:00:00,2504177,SANOCHEM. PHARMAZEUTIKA,0.08256880733944941)
DailyBiggestWinner(2018-02-13 00:00:00,2505025,ARCANDOR AG O.N.,0.25)
DailyBiggestWinner(2018-02-14 00:00:00,2504585,4SC AG,0.08714918759231917)
------------------------------

Found 20 daily biggest winners by joining in 30278 ms.
--------JOINING SAMPLE--------
DailyBiggestWinner(2018-02-01 00:00:00,2505107,BEATE UHSE AG,0.44444444444444453)
DailyBiggestWinner(2018-02-02 00:00:00,2506525,MATTEL INC.          DL 1,0.1885964912280702)
DailyBiggestWinner(2018-02-05 00:00:00,2505364,AIR BERLIN PLC    EO -;25,0.10803324099722994)
DailyBiggestWinner(2018-02-06 00:00:00,2505100,PVA TEPLA AG O.N.,0.1869918699186991)
DailyBiggestWinner(2018-02-07 00:00:00,2505025,ARCANDOR AG O.N.,0.25)
DailyBiggestWinner(2018-02-08 00:00:00,2506580,TWITTER INC.   DL-;000005,0.18804597701149425)
DailyBiggestWinner(2018-02-09 00:00:00,2505025,ARCANDOR AG O.N.,0.25)
DailyBiggestWinner(2018-02-12 00:00:00,2504177,SANOCHEM. PHARMAZEUTIKA,0.08256880733944941)
DailyBiggestWinner(2018-02-13 00:00:00,2505025,ARCANDOR AG O.N.,0.25)
DailyBiggestWinner(2018-02-14 00:00:00,2504585,4SC AG,0.08714918759231917)
------------------------------

Count of records ONLY in sessionizing data set: 0
Count of records ONLY in joining data set: 0
Total diff count: 0
---------DIFF SAMPLE----------
------------------------------


/*****************************
     DAILY BIGGEST VOLUMES
*****************************/

Found 20 daily biggest volumes by sessionizing in 1539 ms.
-----SESSIONIZING SAMPLE------
DailyBiggestVolume(2018-02-01 00:00:00,2505076,DAIMLER AG NA O.N.,387.4346595899998)
DailyBiggestVolume(2018-02-02 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,470.5393311520006)
DailyBiggestVolume(2018-02-05 00:00:00,2505088,SIEMENS AG NA,276.33983692000004)
DailyBiggestVolume(2018-02-06 00:00:00,2505088,SIEMENS AG NA,488.8750247400004)
DailyBiggestVolume(2018-02-07 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,405.53769360399997)
DailyBiggestVolume(2018-02-08 00:00:00,2505088,SIEMENS AG NA,328.26816634)
DailyBiggestVolume(2018-02-09 00:00:00,2504664,BAYER AG NA O.N.,376.89918174999985)
DailyBiggestVolume(2018-02-12 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,311.64534618199986)
DailyBiggestVolume(2018-02-13 00:00:00,2505077,SAP SE O.N.,186.5301399500002)
DailyBiggestVolume(2018-02-14 00:00:00,2505088,SIEMENS AG NA,288.41335660000016)
------------------------------

Found 20 daily biggest volumes by joining in 11570 ms.
--------JOINING SAMPLE--------
DailyBiggestVolume(2018-02-01 00:00:00,2505076,DAIMLER AG NA O.N.,387.4346595899998)
DailyBiggestVolume(2018-02-02 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,470.5393311520006)
DailyBiggestVolume(2018-02-05 00:00:00,2505088,SIEMENS AG NA,276.33983692000004)
DailyBiggestVolume(2018-02-06 00:00:00,2505088,SIEMENS AG NA,488.8750247400004)
DailyBiggestVolume(2018-02-07 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,405.53769360399997)
DailyBiggestVolume(2018-02-08 00:00:00,2505088,SIEMENS AG NA,328.26816634)
DailyBiggestVolume(2018-02-09 00:00:00,2504664,BAYER AG NA O.N.,376.89918174999985)
DailyBiggestVolume(2018-02-12 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,311.64534618199986)
DailyBiggestVolume(2018-02-13 00:00:00,2505077,SAP SE O.N.,186.5301399500002)
DailyBiggestVolume(2018-02-14 00:00:00,2505088,SIEMENS AG NA,288.41335660000016)
------------------------------

Count of records ONLY in sessionizing data set: 0
Count of records ONLY in joining data set: 0
Total diff count: 0
---------DIFF SAMPLE----------
------------------------------


/*****************************
     MOST TRADED STOCK/ETF
*****************************/

Found 2446 security volumes in 2378 ms.
--------SINGLE SAMPLE---------
SecurityVolume(2505025,ARCANDOR AG O.N.,0.024481245075507377)
SecurityVolume(2506097,3W POWER S.A. EO -;01,0.018309049977324028)
SecurityVolume(2506375,STOCKHOLM IT VENTURES AB,0.012936910917439001)
SecurityVolume(2504510,SCY BETEILIG.AG  O.N.,0.011655418248824828)
SecurityVolume(2504346,MYBET HOLDING SE NA O.N.,0.01128141318683449)
SecurityVolume(2504559,SOLVESTA AG,0.011194029850746228)
SecurityVolume(2505065,PRO DV AG O.N.,0.010768477728830148)
SecurityVolume(2505364,AIR BERLIN PLC    EO -;25,0.009728467368176114)
SecurityVolume(2505142,EBRO FOODS NOM. EO -;60,0.009615384615384581)
SecurityVolume(2505107,BEATE UHSE AG,0.009397740800824493)
------------------------------


This application completed in 86992 ms.
~~~

## Observations
Notice that in both cases where we compared methodologies, there were no differences between
them (same row counts and no differing rows). Additionally, the sessionization completed
in much less time (roughly 10x, an order of magnitude). This is most likely because the
sessionization logic only shuffles the data once (at least, until the sorting step),
then maps through each group once to update a state, which it then outputs. Contrarily,
the join logic requires multiple shuffle, map, and reduce steps.

Given that the outputs are identical for both methodologies in each analysis, and
sessionization is much more performant, it seems that sessionization is the way to go
(at least, for this dataset, and given the current constraints). 
