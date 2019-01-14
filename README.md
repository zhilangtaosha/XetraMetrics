# XetraMetrics

#### Description
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

#### High-Level Overview
The task was to implement several SQL queries from
https://github.com/Deutsche-Boerse/dbg-pds/tree/master/examples/sql (located in the
resources directory), each of which handles one of the above analyses, using the Apache
Spark API. I modified the queries a bit so that the referenced fields match the XETRA
schema and the queries would therefore be able to run successfully. (I also added a
"uniqueIdentifier" field, so I would be able to verify the outputs, the approach for
which is listeb below.)

For each analysis, this application first runs the related query in Spark SQL. I am
considering this output the control dataset, which I will compare to the outputs of
the other approaches, written using the Spark API, to check for accuracy and correctness.

For the first two analyses, this application compares two Spark Dataset API methodologies:
one with joins and groups, designed to implement the queries exactly as they are, and
sessionization. It compares the outputs of both implementations to each other and the
conrol output. The third analysis is not eligible for this comparison, as the related
example query does not employ any join logic; however, it still compares the output
to the control.

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

Copy the directory `data-set/` wherever you want, either on your local machine or in HDFS.

## Compiling and packaging the application
~~~
mvn clean package
~~~

## Running the application
#### Running in your IDE
Make sure to add a parameter for the file path of the data (`/path/to/data-set`).

#### Running the jar
You'll want to run in client mode so you can see the output. Make sure to add a parameter
for the file path of the data. This is the same, whether you're running locally or on a
cluster.
~~~
PATH=/path/to/data-set

spark2-submit \
--class com.acorns.techtest.XetraMetrics \
--deploy-mode client \
xetra-metrics-1.0.0-SNAPSHOT.jar \
-p $PATH
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

Found 20 records by sessionizing in 2216 ms.
-----SESSIONIZING SAMPLE------
[2018-02-01 00:00:00.2505107,2018-02-01 00:00:00,2505107,BEATE UHSE AG,0.4444]
[2018-02-02 00:00:00.2506525,2018-02-02 00:00:00,2506525,MATTEL INC.          DL 1,0.1886]
[2018-02-05 00:00:00.2505364,2018-02-05 00:00:00,2505364,AIR BERLIN PLC    EO -;25,0.108]
[2018-02-06 00:00:00.2505100,2018-02-06 00:00:00,2505100,PVA TEPLA AG O.N.,0.187]
[2018-02-07 00:00:00.2505025,2018-02-07 00:00:00,2505025,ARCANDOR AG O.N.,0.25]
[2018-02-08 00:00:00.2506580,2018-02-08 00:00:00,2506580,TWITTER INC.   DL-;000005,0.188]
[2018-02-09 00:00:00.2505025,2018-02-09 00:00:00,2505025,ARCANDOR AG O.N.,0.25]
[2018-02-12 00:00:00.2504177,2018-02-12 00:00:00,2504177,SANOCHEM. PHARMAZEUTIKA,0.0826]
[2018-02-13 00:00:00.2505025,2018-02-13 00:00:00,2505025,ARCANDOR AG O.N.,0.25]
[2018-02-14 00:00:00.2504585,2018-02-14 00:00:00,2504585,4SC AG,0.0871]
------------------------------

Found 20 records by joining in 929 ms.
--------JOINING SAMPLE--------
[2018-02-01 00:00:00.2505107,2018-02-01 00:00:00,2505107,BEATE UHSE AG,0.4444]
[2018-02-02 00:00:00.2506525,2018-02-02 00:00:00,2506525,MATTEL INC.          DL 1,0.1886]
[2018-02-05 00:00:00.2505364,2018-02-05 00:00:00,2505364,AIR BERLIN PLC    EO -;25,0.108]
[2018-02-06 00:00:00.2505100,2018-02-06 00:00:00,2505100,PVA TEPLA AG O.N.,0.187]
[2018-02-07 00:00:00.2505025,2018-02-07 00:00:00,2505025,ARCANDOR AG O.N.,0.25]
[2018-02-08 00:00:00.2506580,2018-02-08 00:00:00,2506580,TWITTER INC.   DL-;000005,0.188]
[2018-02-09 00:00:00.2505025,2018-02-09 00:00:00,2505025,ARCANDOR AG O.N.,0.25]
[2018-02-12 00:00:00.2504177,2018-02-12 00:00:00,2504177,SANOCHEM. PHARMAZEUTIKA,0.0826]
[2018-02-13 00:00:00.2505025,2018-02-13 00:00:00,2505025,ARCANDOR AG O.N.,0.25]
[2018-02-14 00:00:00.2504585,2018-02-14 00:00:00,2504585,4SC AG,0.0871]
------------------------------

There are 0 different records between the sessionizing, joining, and control datasets.


/*****************************
     DAILY BIGGEST VOLUMES
*****************************/

Found 20 records by sessionizing in 1298 ms.
-----SESSIONIZING SAMPLE------
[2018-02-01 00:00:00.2505076,2018-02-01 00:00:00,2505076,DAIMLER AG NA O.N.,387.4347]
[2018-02-02 00:00:00.2504888,2018-02-02 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,470.5393]
[2018-02-05 00:00:00.2505088,2018-02-05 00:00:00,2505088,SIEMENS AG NA,276.3398]
[2018-02-06 00:00:00.2505088,2018-02-06 00:00:00,2505088,SIEMENS AG NA,488.875]
[2018-02-07 00:00:00.2504888,2018-02-07 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,405.5377]
[2018-02-08 00:00:00.2505088,2018-02-08 00:00:00,2505088,SIEMENS AG NA,328.2682]
[2018-02-09 00:00:00.2504664,2018-02-09 00:00:00,2504664,BAYER AG NA O.N.,376.8992]
[2018-02-12 00:00:00.2504888,2018-02-12 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,311.6453]
[2018-02-13 00:00:00.2505077,2018-02-13 00:00:00,2505077,SAP SE O.N.,186.5301]
[2018-02-14 00:00:00.2505088,2018-02-14 00:00:00,2505088,SIEMENS AG NA,288.4134]
------------------------------

Found 20 records by joining in 524 ms.
--------JOINING SAMPLE--------
[2018-02-01 00:00:00.2505076,2018-02-01 00:00:00,2505076,DAIMLER AG NA O.N.,387.4347]
[2018-02-02 00:00:00.2504888,2018-02-02 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,470.5393]
[2018-02-05 00:00:00.2505088,2018-02-05 00:00:00,2505088,SIEMENS AG NA,276.3398]
[2018-02-06 00:00:00.2505088,2018-02-06 00:00:00,2505088,SIEMENS AG NA,488.875]
[2018-02-07 00:00:00.2504888,2018-02-07 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,405.5377]
[2018-02-08 00:00:00.2505088,2018-02-08 00:00:00,2505088,SIEMENS AG NA,328.2682]
[2018-02-09 00:00:00.2504664,2018-02-09 00:00:00,2504664,BAYER AG NA O.N.,376.8992]
[2018-02-12 00:00:00.2504888,2018-02-12 00:00:00,2504888,DEUTSCHE BANK AG NA O.N.,311.6453]
[2018-02-13 00:00:00.2505077,2018-02-13 00:00:00,2505077,SAP SE O.N.,186.5301]
[2018-02-14 00:00:00.2505088,2018-02-14 00:00:00,2505088,SIEMENS AG NA,288.4134]
------------------------------

There are 0 unique records among the sessionizing, joining, and control datasets.


/*****************************
     MOST TRADED STOCK/ETF
*****************************/

Found 2446 records by sessionizing in 1457 ms.
-----SESSIONIZING SAMPLE------
[2505025,2505025,ARCANDOR AG O.N.,0.0245]
[2506097,2506097,3W POWER S.A. EO -;01,0.0183]
[2506375,2506375,STOCKHOLM IT VENTURES AB,0.0129]
[2504510,2504510,SCY BETEILIG.AG  O.N.,0.0117]
[2504346,2504346,MYBET HOLDING SE NA O.N.,0.0113]
[2504559,2504559,SOLVESTA AG,0.0112]
[2505065,2505065,PRO DV AG O.N.,0.0108]
[2505364,2505364,AIR BERLIN PLC    EO -;25,0.0097]
[2505142,2505142,EBRO FOODS NOM. EO -;60,0.0096]
[2505107,2505107,BEATE UHSE AG,0.0094]
------------------------------

There are 0 unique records among the sessionizing, and control datasets.


This application completed in 128346 ms.
~~~

## Observations
Notice that in all three cases, there were no unique records among the output datasets of
the approaches used and the control groups. Additionally, the application completed in 
roughly 128 seconds. Finally, and perhaps most interestingly, the sessionization completed
in much less time (roughly 10x, an order of magnitude). This is most likely because the
sessionization logic only shuffles the data once (at least, until the sorting step),
then maps through each group once to update a state, which it then outputs. Contrarily,
the join logic requires multiple shuffle, map, and reduce steps.

Given that the outputs are identical for both methodologies in each analysis, and
sessionization is much more performant, it seems that sessionization is the way to go
(at least, for this dataset, and given the current constraints).
