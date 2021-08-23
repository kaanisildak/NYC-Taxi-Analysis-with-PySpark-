from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import desc
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp

conf = SparkConf().setAppName("Homework4")
spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()
sc = spark.sparkContext
##### WARNING!!!! Unfortunately, the function call does not work here in this document. When we did the questions,
# we worked on another file where we did it customely for each question. It ended up not working here. -_-
# In order to run these codes, these steps should be followed :
# 1- Open a new .py file and write the libraries above.
# 2- Then write the if __name__ == '__main__': part down below.
# 3- After that, please kindly write each of these functions' codes and run them. When you are done with one, delete
# the respective part and write the next function's code. :(
# We are very sorry for this inconvenience.

def find_statistical_information(df):

    for x in green_df.columns:
        green_df.agg({x: 'max'}).show()
        green_df.agg({x: 'min'}).show()
        green_df.agg({x: 'average'}).show()
        green_df.agg({x: 'mean'}).show()
    for x in yellow_df.columns:
        yellow_df.agg({x: 'max'}).show()
        yellow_df.agg({x: 'min'}).show()
        yellow_df.agg({x: 'average'}).show()
        yellow_df.agg({x: 'mean'}).show()
def join_look_up_with_cities(df):
        green_df.join(lookup_df, green_df.PULocationID == lookup_df.LocationID, how='inner')
        green_df.join(lookup_df, green_df.DOLocationID == lookup_df.LocationID, how='inner')

        yellow_df.join(lookup_df, yellow_df.PULocationID == lookup_df.LocationID, how='inner')
        yellow_df.join(lookup_df, yellow_df.DOLocationID == lookup_df.LocationID, how='inner')

def get_most_expensive_route(df):

    # For dataset green:
    df.sort('total_amount', ascending=False).show() # This gives us the starting point which is Downtown Brooklyn.
    df2.sort('total_amount', ascending=False).show()  # This gives us the end point which is Queens.
    # Hence most expensive route is Downtown Brooklyn > Queens, JFK Airport .

    # For dataset yellow:
    df3.sort('total_amount', ascending=False).show() # There are many most expensive routes so let's pick Manhattan, Clinton East as our starting point.
    df4.sort('total_amount', ascending=False).show() # Here we see that our end point is EWR, Newark Airport.
    # Hence most expensive route is Manhattan, Clinton East > EWR, Newark Airport .

def get_busiest_taxi_station(df):
    df9 = df3.groupBy('service_zone').count()
    df9.sort("count", ascending=False).show(1) # Gives us the busiest taxi station for yellow: Yellow Zone.

    df8 = df.groupBy('service_zone').count()
    df8.sort("count", ascending=False).show(1) # Gives us the busiest taxi station for green: Boro Zone.


def get_top_5_busiest_area(df):
    # green: East Harlem North,East Harlem South, Central Harlem,Central Harlem North, Elmhurst

    df2 = df.groupBy('Zone').count()
    df2.sort("count", ascending=False).show(5)

    #yellow:Upper East Side South, Upper East Side North, Midtown Center, Midtown East, Penn Station/Madi..
    df5 = df3.groupBy('Zone').count()
    df5.sort("count", ascending=False).show(5)

def get_longest_trips(df):
    df.select(df.lpep_pickup_datetime, (unix_timestamp(df.lpep_dropoff_datetime) - unix_timestamp(df.lpep_pickup_datetime)).alias('diff'),df.PULocationID, df.DOLocationID)\
    .sort("diff", ascending=False).show(1)
#green
#+--------------------+-----+------------+------------+
#|lpep_pickup_datetime| diff|PULocationID|DOLocationID|
#+--------------------+-----+------------+------------+
#| 2020-03-05 12:18:30|86392|         181|          62|

    df3.select(df3.tpep_pickup_datetime, (unix_timestamp(df3.tpep_dropoff_datetime) - unix_timestamp(df3.tpep_pickup_datetime)).alias('diff'),df3.PULocationID, df3.DOLocationID)\
    .sort("diff", ascending=False).show(1)
# yellow
#+--------------------+------+------------+------------+
#|tpep_pickup_datetime|  diff|PULocationID|DOLocationID|
#+--------------------+------+------------+------------+
#| 2020-03-05 12:46:56|108889|         238|         263|
#+--------------------+------+------------+------------+###
def get_crowded_places_per_hour(df):
    # Green:

    df_new = df.withColumn("hour", hour(col("lpep_pickup_datetime")))
    df15 = df_new.groupBy('PULocationID','hour','Zone').count()
    df15.sort("count", ascending=False).show()

    df_new = df2.withColumn("hour", hour(col("tpep_pickup_datetime")))
    df16 = df_new.groupBy('PULocationID','hour','Zone').count()
    df16.sort("count", ascending=False).show()

    #Yellow:
    df_new = df3.withColumn("hour", hour(col("lpep_pickup_datetime")))
    df15 = df_new.groupBy('PULocationID','hour','Zone').count()
    df15.sort("count", ascending=False).show()

    df_new = df3.withColumn("hour", hour(col("tpep_pickup_datetime")))
    df15 = df_new.groupBy('PULocationID','hour','Zone').count()
    df15.sort("count", ascending=False).show()
def get_busiest_hours(df):
    """
    Find the Pickup and Drop-off count for each hour. After that draw two lineplot graphs for Pickup and Drop-off.
    This function returns busiest hour that you will use it in draw_busiest_hours_graph to draw lineplot graph.
    Hint**: You can use window functions.
    :param df: Dataframe
    :return: Dataframe
    """


def draw_busiest_hours_graph(df):
    """
    You will use get_busiest_hours' result here. With this dataframe you should draw hour to count lineplot.
    Hint**: You can convert Spark Dataframe to Pandas Dataframe here.
    :param df: Dataframe
    """


def get_tip_correlation(df):
    for x in df.columns:
        df.stat.corr(str(df.tip_amount),str(df.trip_distance))
# doesnt work because there is whitespace in the column :-(

if __name__ == '__main__':
    green_df = spark.read.csv(path='green_tripdata_2020-03.csv', sep=',', header=True,
                              schema=(
                                  'VendorID STRING, lpep_pickup_datetime STRING, lpep_dropoff_datetime STRING, store_and_fwd_flag  STRING, RatecodeID STRING, PULocationID STRING, DOLocationID STRING , passenger_count STRING, trip_distance STRING, fare_amount STRING, extra STRING, mta_tax STRING , tip_amount STRING, tolls_amount STRING , ehail_fee STRING, improvement_surcharge STRING, total_amount STRING, payment_type STRING, trip_type STRING, congestion_surcharge STRING'))

    yellow_df = spark.read.csv(path='yellow_tripdata_2020-03.csv', sep=',', header=True,
                               schema=(
                                   'VendorID STRING,tpep_pickup_datetime STRING,tpep_dropoff_datetime STRING,passenger_count STRING,trip_distance STRING,RatecodeID STRING,store_and_fwd_flag STRING,PULocationID STRING,DOLocationID STRING,payment_type STRING,fare_amount STRING,extra STRING,mta_tax STRING,tip_amount STRING,tolls_amount STRING,improvement_surcharge STRING,total_amount STRING,congestion_surcharge STRING'))

    lookup_df = spark.read.csv(path='yellow_tripdata_2020-03.csv', sep=',', header=True,
                               schema=('LocationID STRING, Borough STRING,Zone STRING,service_zone STRING'))

    df = green_df.join(lookup_df, green_df.PULocationID == lookup_df.LocationID, how='inner')
    df2 = green_df.join(lookup_df, green_df.DOLocationID == lookup_df.LocationID, how='inner')

    df3 = yellow_df.join(lookup_df, yellow_df.PULocationID == lookup_df.LocationID, how='inner')
    df4 = yellow_df.join(lookup_df, yellow_df.DOLocationID == lookup_df.LocationID, how='inner')

