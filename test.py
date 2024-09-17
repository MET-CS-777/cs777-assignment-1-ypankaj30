from __future__ import print_function

import os
import platform
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *
from pyspark.sql.window import Window


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

def cleanup(df):
    df = df.rdd.map(lambda x: x).filter(correctRows).toDF()
    return df

#Task 1 - Top-10 Active Taxis
def top10_active_taxis(df):
    # group by taxis medallion and count distinct hack_license
    # sort by count and get top 10
    df = df.groupBy("medallion").agg(func.countDistinct("hack_license").alias("distinct_drivers")).sort(func.desc("distinct_drivers")).limit(10)

    return df

#Main
if __name__ == "__main__":
    cols = ["medallion", "hack_license", "pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total amount"]
    # For local testing
    is_local = False
    if platform.system() == 'Darwin':
        is_local = True
    
    #is_local = False
    if is_local == True:
        #Local mode
        file_name = "file:/Users/pankajyawale/Documents/Pankaj/BostonUniversity/bu-work/cs777-bigdata-analysis/assignments/1/work/cs777-assignment-1-ypankaj30/taxi-data-sorted-small.csv.bz2"
        # Authenticate using this command to access the file from GCS on local
        # Didn't work
        # gcloud auth application-default login
        #file_name = "gs://met-cs-777-data/taxi-data-sorted-small.csv.bz2"
    else:
        file_name = "gs://met-cs-777-data/taxi-data-sorted-small.csv.bz2"

    # Read CSV from local for local testing
    print("Reading file: ", file_name)
    spark = SparkSession.builder.appName("Assignment-1").config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar").getOrCreate()
    df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(file_name)
    df = df.toDF(*cols)
    df = cleanup(df.sample(0.05))
    df.persist()

    top10_active_taxis_df = top10_active_taxis(df)
    print("Top 10 Active Taxis")
    spark.setlocalproperty("callSite.short", "Top 10 Active Taxis")
    spark.setlocalproperty("callSite.long", "Top 10 Active Taxis")
    top10_active_taxis_df.show(10, False)

