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

#Task 2 - Top-10 Best Drivers
def top10_best_driver(df):
    # group by hack_license and calculate average amount per minute
    # sort by sum and get top 10
    df = df.groupBy("hack_license").agg(func.sum("total amount").alias("total_amount"), func.sum("trip_time_in_secs").alias("total_time_secs")).withColumn("amount_per_minute", func.round(func.col("total_amount")/func.col("total_time_secs")*60, 2)).sort(func.desc("amount_per_minute")).limit(10)

    return df

#Task 3 - Best Time of Day
def best_time_of_day(df):
    # group by hour from pickup_datetime and sum up surcharge and trip_distance
    # calculate profit ratio = (Surcharge Amount in US Dollar) / (Travel Distance in miles).
    # Find hour with highest profit ratio
    df = df.withColumn("hour", hour("pickup_datetime")).groupBy("hour").agg(func.sum("surcharge").alias("total_surcharge"), func.round(func.sum("trip_distance"),2).alias("total_distance")).withColumn("profit_ratio", func.round(func.col("total_surcharge")/func.col("total_distance"), 2)).sort(func.desc("profit_ratio")).limit(1)

    return df

#Task 4.1 - Cash vs card payers per hour
def cash_vs_card_payers_per_hour(df):
    # Found payment type as UNK in some rows. Filter those
    df = df.filter(~df["payment_type"].isin(["UNK","NOC","DIS"]))

    # group by hour from pickup_datetime and payment_type and count total number of cash and card payers
    df = df.withColumn("hour", hour("pickup_datetime")).groupBy("hour", "payment_type").agg(func.count("payment_type").alias("total_count"))
    #df.show(df.count(), False)

    # pivot the data to get cash and card payers in separate columns as percentage of total per hour

    df = df.groupBy("hour").pivot("payment_type").agg(func.first("total_count"))

    # calculate percentage of cash and card payers per hour
    df = df.withColumn("total", func.col("CSH")+func.col("CRD")).withColumn("Cash", func.round(func.col("CSH")/func.col("total")*100, 2)).withColumn("Credit card", func.round(func.col("CRD")/func.col("total")*100, 2)).drop("total", "CSH", "CRD").sort("hour")

    return df

# Task 4.2 - Top 10 efficient drivers
def top10_efficient_drivers(df):
    # Filter trips with distance > 1 mile as there are some trips with less than 1 mile distance
    df = df.filter(df["trip_distance"] > 1)
    # group by hack_license and calculate average amount per mile
    # sort by sum and get top 10
    df = df.groupBy("hack_license").agg(func.sum("total amount").alias("total_amount"), func.round(func.sum("trip_distance"),2).alias("total_distance")).withColumn("amount_per_mile", func.round(func.col("total_amount")/func.col("total_distance"), 2)).sort(func.desc("amount_per_mile")).limit(10)

    return df

# Task 4.3 - Calculate mean, median, first and third quntilesof trip ampount
def trip_amount_stats(df):
    # Calculate mean, median, first and third quartile of trip amount
    df = df.select(func.mean("total amount").alias("mean"), func.expr("percentile_approx(`total amount`, 0.5)").alias("median"), func.expr("percentile_approx(`total amount`, 0.25)").alias("first_quartile"), func.expr("percentile_approx(`total amount`, 0.75)").alias("third_quartile"))

    return df

# Task 4.4 - Using the IQR outlier detection method, find out the top 10 outliers.
def top10_outliers(df):
    # Calculate IQR
    q1 = df.selectExpr("percentile_approx(`total amount`, 0.25)").collect()[0][0]
    q3 = df.selectExpr("percentile_approx(`total amount`, 0.75)").collect()[0][0]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    # Find outliers
    df = df.filter((df["total amount"] < lower_bound) | (df["total amount"] > upper_bound)).sort("total amount", ascending=False).limit(10).select("medallion", "hack_license", "total amount")

    return df

#Main
if __name__ == "__main__":
    cols = ["medallion", "hack_license", "pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total amount"]
    # For local testing
    is_local = False
    if platform.system() == 'Darwin':
        is_local = True
    
    if is_local == True:
        #Local mode
        file_name = "file:/Users/pankajyawale/Documents/Pankaj/BostonUniversity/bu-work/cs777-bigdata-analysis/assignments/1/work/cs777-assignment-1-ypankaj30/taxi-data-sorted-small.csv.bz2"
    else:
        file_name = "gs://met-cs-777-data/taxi-data-sorted-large.csv.bz2"

    # Read CSV from local for local testing
    print("Reading file: ", file_name)
    spark = SparkSession.builder.appName("Assignment-1").config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar").getOrCreate()
    spark.sparkContext.setLocalProperty("callSite.short", "Loading Taxi Data: " + file_name)
    df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(file_name)
    df = df.toDF(*cols)
    #print("Total Records: ", df.count())
    #1999999

    spark.sparkContext.setLocalProperty("callSite.short", "Cleaning Taxi Data")
    if is_local == True:
        df = cleanup(df.sample(0.05))
    else:
        df = cleanup(df)

    spark.sparkContext.setLocalProperty("callSite.short", "Caching Taxi Data")
    df.persist()
    #print("Total Records after cleanup: ", df.count())
    #1971891

    #Task 1 - Top-10 Active Taxis
    #Your code goes here

    top10_active_taxis_df = top10_active_taxis(df)
    print("Top 10 Active Taxis")
    spark.sparkContext.setLocalProperty("callSite.short", "Top 10 Active Taxis")
    spark.sparkContext.setLocalProperty("callSite.long", "Top 10 Active Taxis")
    top10_active_taxis_df.show(10, False)

    #Task 2
    #Your code goes here
    top10_best_driver_df = top10_best_driver(df)
    print("Top 10 Best Drivers")
    spark.sparkContext.setLocalProperty("callSite.short", "Top 10 Best Drivers")
    spark.sparkContext.setLocalProperty("callSite.long", "Top 10 Best Drivers")
    top10_best_driver_df.show(10, False)

    #savings output to argument


    #Task 3 - Optional 
    #Your code goes here
    best_time_of_day_df = best_time_of_day(df)
    print("Best Time of Day")
    spark.sparkContext.setLocalProperty("callSite.short", "Best Time of Day")
    spark.sparkContext.setLocalProperty("callSite.long", "Best Time of Day")
    best_time_of_day_df.show(1, False)

    #Task 4 - Optional 
    #Your code goes here

    #4.1 - Cash vs card payers per hour
    cash_vs_card_payers_per_hour_df = cash_vs_card_payers_per_hour(df)
    print("Cash vs Card Payers per Hour")
    spark.sparkContext.setLocalProperty("callSite.short", "Cash vs Card Payers per Hour")
    spark.sparkContext.setLocalProperty("callSite.long", "Cash vs Card Payers per Hour")
    cash_vs_card_payers_per_hour_df.show(cash_vs_card_payers_per_hour_df.count(), False)

    # Task 4.2 - Top 10 efficient drivers
    top10_efficient_drivers_df = top10_efficient_drivers(df)
    print("Top 10 Efficient Drivers")
    spark.sparkContext.setLocalProperty("callSite.short", "Top 10 Efficient Drivers")
    spark.sparkContext.setLocalProperty("callSite.long", "Top 10 Efficient Drivers")
    top10_efficient_drivers_df.show(10, False)

    # Task 4.3 - Calculate mean, median, first and third quartile of trip amount
    trip_amount_stats_df = trip_amount_stats(df)
    print("Trip Amount Stats")
    spark.sparkContext.setLocalProperty("callSite.short", "Trip Amount Stats")
    spark.sparkContext.setLocalProperty("callSite.long", "Trip Amount Stats")
    trip_amount_stats_df.show(5, False)

    # Task 4.4 - Using the IQR outlier detection method, find out the top 10 outliers.
    top10_outliers_df = top10_outliers(df)
    print("Top 10 Outliers")
    spark.sparkContext.setLocalProperty("callSite.short", "Top 10 Outliers")
    spark.sparkContext.setLocalProperty("callSite.long", "Top 10 Outliers")
    top10_outliers_df.show(10, False)