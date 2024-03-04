# Data Engineering Zoomcamp Module 5 - Batch Processing with Spark
In this module, we learn how to operate batch processing using Spark which inclue loading the data, wrangling data using SQL feature in Spark, and also learn how Spark partitioning the data we use.

## Homework Solution

### 1. Install Spark and PySpark
```
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
pd.DataFrame.iteritems = pd.DataFrame.items

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

spark.version
```
the answer is:
```
'3.3.2'
```

### 2. What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

first we download the data:
```
!wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz
```

then we create the schema:
```
types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropOff_datetime', types.TimestampType(), True), 
    types.StructField('PUlocationID', types.IntegerType(), True), 
    types.StructField('DOlocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.DoubleType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
])
```

after that we build the spark dataframe using the data that we donwloaded and the schema we created earlier:
```
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhv_tripdata_2019-10.csv')
```

and we partition and save the data:
```
df \
    .repartition(6) \
    .write.parquet('data/fhv/homework', mode='overwrite')
```

to check the size of each partition, we go to the folder and do this command in terminal:
```
ls -l
```
and the result is:
```
total 36904
-rw-r--r-- 1 yudisyudis yudisyudis       0 Mar  5 03:47 _SUCCESS
-rw-r--r-- 1 yudisyudis yudisyudis 6289459 Mar  5 03:47 part-00000-ecf3d8de-b032-4694-97bd-3359eaa52727-c000.snappy.parquet
-rw-r--r-- 1 yudisyudis yudisyudis 6298806 Mar  5 03:47 part-00001-ecf3d8de-b032-4694-97bd-3359eaa52727-c000.snappy.parquet
-rw-r--r-- 1 yudisyudis yudisyudis 6296797 Mar  5 03:47 part-00002-ecf3d8de-b032-4694-97bd-3359eaa52727-c000.snappy.parquet
-rw-r--r-- 1 yudisyudis yudisyudis 6293119 Mar  5 03:47 part-00003-ecf3d8de-b032-4694-97bd-3359eaa52727-c000.snappy.parquet
-rw-r--r-- 1 yudisyudis yudisyudis 6292420 Mar  5 03:47 part-00004-ecf3d8de-b032-4694-97bd-3359eaa52727-c000.snappy.parquet
-rw-r--r-- 1 yudisyudis yudisyudis 6304541 Mar  5 03:47 part-00005-ecf3d8de-b032-4694-97bd-3359eaa52727-c000.snappy.parquet
```

### 3. How many taxi trips were there on the 15th of October?

to answer this question, we utilize the SQL feature
first we create a temporary table:
```
df.createOrReplaceTempView('master')
```
then we build an SQL spark script:
```
df_15 = spark.sql("""
    SELECT 
        date_trunc('day', pickup_datetime) AS day,     
        COUNT(1) AS trips
    FROM
        master
    WHERE
        pickup_datetime >= '2019-10-15' AND pickup_datetime < '2019-10-16'
    GROUP BY
        1
""")

df_15.show()
```
the answer is:
```
|2019-10-15 00:00:00|62610|
```

### 4. What is the length of the longest trip in the dataset in hours?

to get an answer in hours, first we load another spark data from the same sources with additional column which contain the duration in hour created using spark function:
```
df_duration = spark.read.parquet('data/fhv/homework')  
df_duration = df_duration.withColumn("duration_in_hours", (F.col("dropOff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 3600)
```
after that we crated another SQL script to find the answer:
```
df_duration = spark.sql("""
    SELECT 
        duration_in_hours
    FROM
        duration
    ORDER BY
        duration_in_hours DESC
     
""")

df_duration.show()
```
and the answer is:
```
 duration_in_hours|
+------------------+
|          631152.5|
|          631152.5|
| 87672.44083333333|
| 70128.02805555555|
|            8794.0|
| 8784.166666666666|
|1464.5344444444445|
|1056.8266666666666|
|1056.2705555555556|
| 793.5530555555556|
| 793.3858333333334|
|          793.2975|
| 792.9980555555555|
| 792.9883333333333|
| 792.8602777777778|
| 792.8108333333333|
|           792.785|
| 792.7694444444444|
| 792.7538888888889|
| 792.7463888888889|
+------------------+
only showing top 20 rows
```

### 5. Spark’s User Interface which shows the application's dashboard runs on which local port?
during my learning, spark alway operate in port 4040, or 4041

### 6. Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?

to simplify, because the question is just about the pickup zone, we only joining the data once, between pickup location id in fhv data and zone data
first, we load the zone data that we have already downloaded earlier:

```
df_zone = spark.read.parquet('zones/')
```

then join the data:

```
df_join = df.join(df_zone, df.PUlocationID == df_zone.LocationID)
df_join.show()
```

after that we utilize the SQL to find the answer:

```
df_join.createOrReplaceTempView('pickup')

df_pu = spark.sql("""
    SELECT 
        Zone,
        COUNT(1) as Frequency
    FROM
        pickup
    GROUP BY
        1
    ORDER BY 
        Frequency ASC
     
""")

df_pu.show()
```

the answer is:

```
Zone|Frequency|
+--------------------+---------+
|         Jamaica Bay|        1|
|Governor's Island...|        2|
| Green-Wood Cemetery|        5|
|       Broad Channel|        8|
|     Highbridge Park|       14|
|        Battery Park|       15|
|Saint Michaels Ce...|       23|
|Breezy Point/Fort...|       25|
|Marine Park/Floyd...|       26|
|        Astoria Park|       29|
|    Inwood Hill Park|       39|
|       Willets Point|       47|
|Forest Park/Highl...|       53|
|  Brooklyn Navy Yard|       57|
|        Crotona Park|       62|
|        Country Club|       77|
|     Freshkills Park|       89|
|       Prospect Park|       98|
|     Columbia Street|      105|
|  South Williamsburg|      110|
+--------------------+---------+
only showing top 20 rows
```
