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
