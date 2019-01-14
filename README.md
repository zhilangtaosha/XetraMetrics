# XetraMetrics

#### Description
The overall goal of this project is to run three analyses against the
public XETRA dataset from Deutsche Bank (publicly available at
s3://deutsche-boerse-xetra-pds on AWS or at
https://drive.google.com/open?id=1QmcwvzyWp7RBwPSagQoIbbZqe_6E9jaC on
Google Drive):
1. Daily biggest winners: one record per date, sorted by date, representing the
stock/fund with the largest gain for the given date
2. Daily biggest volumes: one record per date, sorted by date, representing the
stock/fund with the highest trade volume for the given date
3. Most traded stock/etf: one record per stock/fund, sorted in descending order by
the implied volume amount for each stock

#### High-Level Overview
The task was to implement several SQL queries from
https://github.com/Deutsche-Boerse/dbg-pds/tree/master/examples/sql (located in the
resources directory), each of which handles one of the above analyses, using the Apache
Spark Dataset and/or DataFrame API. I modified the queries a bit so that the referenced
fields match the XETRA schema and the queries would therefore be able to run successfully.
(I also added a "uniqueIdentifier" field, so I would be able to verify the outputs.)

To help accomplish this goal, this project contains three separate applications, each
of which serves a distinct purpose:
1. UAT - This application checks that the output of the data-processing code matches
the output of the SQL query itself.
2. Benchmarking - This application times each analysis implementation to inform the user
which implementation is the fastest.
3. Metrics - This application utilizes the resultant knowledge from the first two
applications (data integrity from UAT and optimization from Benchmarking) to run the metrics
and output to the user.

##### UAT
This application runs each of the three queries itself, stores the output, and compares it
to the outputs of the Spark Dataset/DataFrame API implementations. (It also compares the
outputs of each implementation.) We are looking to see that row counts and actual rows are
the same across all outputs. This helps us because if all the code outputs the same data as
the SQL query, then we can choose the implementation with the best performance, which the
next application helps us determine.

For the first two analyses, this application compares two Spark Dataset API methodologies:
one with joins and groups, designed to implement the queries exactly as they are but using
the Spark Dataset and/or DataFrame API, and sessionization, which also uses these same
API's, but in a different way. (As mentioned above, these outputs are also compared to the
control output.) The third analysis is not eligible for a comparison of joining and
sessionization because the related example query does not employ any join logic; however,
it still compares the output to the control.

##### Benchmarking
This application is relatively simple. It outputs runtime duration for both the joining and
sessionization implementations of the queries. The idea is that the fastest query is probably
the better optimized and therefore more suited for production.

#### Metrics
This application simply runs the metrics code and outputs the data to the console for the user.
After running the UAT and Benchmarking applications, I chose the implementations to run for
each analysis.

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
Make sure to add a parameter for the file path of the data (`/path/to/data-set`). You have to do this
for every class you want to run.

#### Running the jar
Run in client mode so you can see the output. Make sure to specify which class you want to run, and add
a parameter for the file path of the data. This is the same, whether you're running locally or on a
cluster.

~~~
PATH=/path/to/data-set

# To output benchmarking results, use:
CLASSNAME=com.acorns.techtest.XetraBenchmarks

# To output UAT results, use:
CLASSNAME=com.acorns.techtest.XetraMetricsUAT

# To output actual metrics data, use:
CLASSNAME=com.acorns.techtest.XetraMetrics

spark2-submit \
--class $CLASSNAME \
--deploy-mode client \
xetra-metrics-1.0.0-SNAPSHOT.jar \
-p $PATH
~~~

## Testing

#### UAT
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

Found 20 records by sessionizing.
Found 20 records by joining.

There are 0 unique records among the sessionizing, joining, and control datasets.


/*****************************
     DAILY BIGGEST VOLUMES
*****************************/

Found 20 records by sessionizing.
Found 20 records by joining.

There are 0 unique records among the sessionizing, joining, and control datasets.


/*****************************
     MOST TRADED STOCK/ETF
*****************************/

Found 2446 records by sessionizing.

There are 0 unique records between the sessionizing and control datasets.
~~~

#### Benchmarking
~~~
/*****************************
     DAILY BIGGEST WINNERS
*****************************/

Sessionizing logic completed in 12579 ms.
Joining logic completed in 27826 ms.


/*****************************
     DAILY BIGGEST VOLUMES
*****************************/

Sessionizing logic completed in 2731 ms.
Joining logic completed in 10634 ms.


/*****************************
     MOST TRADED STOCK/ETF
*****************************/

Sessionizing logic completed in 2933 ms.


This application completed in 65792 ms.
~~~

### Observations
UAT confirms that for each analysis, both the joining and sessionization is correct. Additionally,
Benchmarking confirms that the sessionization logic is more performant. Therefore, it seems that
we should use the sessionization implementation when actually running each analysis.__*__

*<sub>\*This isn't 100% confirmed, as there are so many constraints that we haven't considered, but
that would apply in a production environment, such as the size of the data. However, for the purposes
of this exercise, I made an educated guess based on my observations.</sub>*

## Outputting the data
Note that for the last analysis, "MOST TRADED STOCK/ETF", only 20 rows appear. This is easy to modify
in the code. However, this is probably unnecessary because the analysis is really about finding the
most traded stocks, using ImpliedVolume. Since this result set is sorted in descending order, the top
20 rows is probably sufficient for painting a picture of the data.

#### Analysis
~~~
+/*****************************
      DAILY BIGGEST WINNERS
 *****************************/
 +--------------------+-------------------+----------+--------------------+-------------+
 |    uniqueIdentifier|               Date|SecurityID|         Description|PercentChange|
 +--------------------+-------------------+----------+--------------------+-------------+
 |2018-02-01 00:00:...|2018-02-01 00:00:00|   2505107|       BEATE UHSE AG|       0.4444|
 |2018-02-02 00:00:...|2018-02-02 00:00:00|   2506525|MATTEL INC.      ...|       0.1886|
 |2018-02-05 00:00:...|2018-02-05 00:00:00|   2505364|AIR BERLIN PLC   ...|        0.108|
 |2018-02-06 00:00:...|2018-02-06 00:00:00|   2505100|   PVA TEPLA AG O.N.|        0.187|
 |2018-02-07 00:00:...|2018-02-07 00:00:00|   2505025|    ARCANDOR AG O.N.|         0.25|
 |2018-02-08 00:00:...|2018-02-08 00:00:00|   2506580|TWITTER INC.   DL...|        0.188|
 |2018-02-09 00:00:...|2018-02-09 00:00:00|   2505025|    ARCANDOR AG O.N.|         0.25|
 |2018-02-12 00:00:...|2018-02-12 00:00:00|   2504177|SANOCHEM. PHARMAZ...|       0.0826|
 |2018-02-13 00:00:...|2018-02-13 00:00:00|   2505025|    ARCANDOR AG O.N.|         0.25|
 |2018-02-14 00:00:...|2018-02-14 00:00:00|   2504585|              4SC AG|       0.0871|
 |2018-02-15 00:00:...|2018-02-15 00:00:00|   2504678|COMMERZBANK ETC UNL.|       0.0874|
 |2018-02-16 00:00:...|2018-02-16 00:00:00|   2504910|ARTEC TECHNOLOGIE...|        0.224|
 |2018-02-19 00:00:...|2018-02-19 00:00:00|   2506375|STOCKHOLM IT VENT...|       0.3333|
 |2018-02-20 00:00:...|2018-02-20 00:00:00|   2505107|       BEATE UHSE AG|       0.2178|
 |2018-02-21 00:00:...|2018-02-21 00:00:00|   2505364|AIR BERLIN PLC   ...|       0.1051|
 |2018-02-22 00:00:...|2018-02-22 00:00:00|   2506097|3W POWER S.A. EO ...|       0.1379|
 |2018-02-23 00:00:...|2018-02-23 00:00:00|   2504341|      ITN NANOVATION|       0.2029|
 |2018-02-26 00:00:...|2018-02-26 00:00:00|   2505364|AIR BERLIN PLC   ...|       0.0973|
 |2018-02-27 00:00:...|2018-02-27 00:00:00|   2504428|  AIXTRON SE NA O.N.|       0.1486|
 |2018-02-28 00:00:...|2018-02-28 00:00:00|   2504436|RIB SOFTWARE SE  ...|       0.1022|
 +--------------------+-------------------+----------+--------------------+-------------+
 
 /*****************************
      DAILY BIGGEST VOLUMES
 *****************************/
 +--------------------+-------------------+----------+--------------------+---------+
 |    uniqueIdentifier|               Date|SecurityID|         Description|MaxAmount|
 +--------------------+-------------------+----------+--------------------+---------+
 |2018-02-01 00:00:...|2018-02-01 00:00:00|   2505076|  DAIMLER AG NA O.N.| 387.4347|
 |2018-02-02 00:00:...|2018-02-02 00:00:00|   2504888|DEUTSCHE BANK AG ...| 470.5393|
 |2018-02-05 00:00:...|2018-02-05 00:00:00|   2505088|       SIEMENS AG NA| 276.3398|
 |2018-02-06 00:00:...|2018-02-06 00:00:00|   2505088|       SIEMENS AG NA|  488.875|
 |2018-02-07 00:00:...|2018-02-07 00:00:00|   2504888|DEUTSCHE BANK AG ...| 405.5377|
 |2018-02-08 00:00:...|2018-02-08 00:00:00|   2505088|       SIEMENS AG NA| 328.2682|
 |2018-02-09 00:00:...|2018-02-09 00:00:00|   2504664|    BAYER AG NA O.N.| 376.8992|
 |2018-02-12 00:00:...|2018-02-12 00:00:00|   2504888|DEUTSCHE BANK AG ...| 311.6453|
 |2018-02-13 00:00:...|2018-02-13 00:00:00|   2505077|         SAP SE O.N.| 186.5301|
 |2018-02-14 00:00:...|2018-02-14 00:00:00|   2505088|       SIEMENS AG NA| 288.4134|
 |2018-02-15 00:00:...|2018-02-15 00:00:00|   2505133|  ALLIANZ SE NA O.N.| 251.0191|
 |2018-02-16 00:00:...|2018-02-16 00:00:00|   2505133|  ALLIANZ SE NA O.N.| 345.4178|
 |2018-02-19 00:00:...|2018-02-19 00:00:00|   2505076|  DAIMLER AG NA O.N.| 237.2267|
 |2018-02-20 00:00:...|2018-02-20 00:00:00|   2505076|  DAIMLER AG NA O.N.|  172.717|
 |2018-02-21 00:00:...|2018-02-21 00:00:00|   2504954|    DT.TELEKOM AG NA| 168.0454|
 |2018-02-22 00:00:...|2018-02-22 00:00:00|   2504954|    DT.TELEKOM AG NA| 314.4766|
 |2018-02-23 00:00:...|2018-02-23 00:00:00|   2505114|VOLKSWAGEN AG VZO...|   259.37|
 |2018-02-26 00:00:...|2018-02-26 00:00:00|   2505076|  DAIMLER AG NA O.N.| 275.6405|
 |2018-02-27 00:00:...|2018-02-27 00:00:00|   2505076|  DAIMLER AG NA O.N.| 239.5214|
 |2018-02-28 00:00:...|2018-02-28 00:00:00|   2504664|    BAYER AG NA O.N.| 369.1912|
 +--------------------+-------------------+----------+--------------------+---------+
 
 /*****************************
      MOST TRADED STOCK/ETF
 *****************************/
 +----------------+----------+--------------------+-------------+
 |uniqueIdentifier|SecurityID|         Description|ImpliedVolume|
 +----------------+----------+--------------------+-------------+
 |         2505025|   2505025|    ARCANDOR AG O.N.|       0.0245|
 |         2506097|   2506097|3W POWER S.A. EO ...|       0.0183|
 |         2506375|   2506375|STOCKHOLM IT VENT...|       0.0129|
 |         2504510|   2504510|SCY BETEILIG.AG  ...|       0.0117|
 |         2504346|   2504346|MYBET HOLDING SE ...|       0.0113|
 |         2504559|   2504559|         SOLVESTA AG|       0.0112|
 |         2505065|   2505065|      PRO DV AG O.N.|       0.0108|
 |         2505364|   2505364|AIR BERLIN PLC   ...|       0.0097|
 |         2505142|   2505142|EBRO FOODS NOM. E...|       0.0096|
 |         2505107|   2505107|       BEATE UHSE AG|       0.0094|
 |         2504850|   2504850|SKW STAHL-MET.HLD...|        0.009|
 |         2504620|   2504620|DF DT.FORFAIT AG ...|       0.0088|
 |         2505099|   2505099|TELES AG INFORM.T...|       0.0081|
 |         2505000|   2505000|MATERNUS-KLI.AG O.N.|       0.0074|
 |         2506442|   2506442|BCO BIL.VIZ.ARG.A...|       0.0068|
 |         2592520|   2592520| FYBER N.V.  EO -;10|       0.0067|
 |         2504908|   2504908|QUIRIN PRIVATBK  ...|       0.0059|
 |         2509633|   2509633|SIRONA BIOCHEM CORP.|       0.0057|
 |         2504341|   2504341|      ITN NANOVATION|       0.0053|
 |         2504378|   2504378|       UET AG   O.N.|       0.0052|
 +----------------+----------+--------------------+-------------+
 only showing top 20 rows
~~~
