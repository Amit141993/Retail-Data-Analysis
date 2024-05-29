## Processing the input data streams into the JSON files

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# SparkSession 
spark = SparkSession  \
    .builder  \
    .appName("RetailDataAnalysis")  \
    .getOrCreate()       
spark.sparkContext.setLogLevel('ERROR')


# Utility Functions

#Checking New order
def is_a_order(type):
   return 1 if type == 'ORDER' else 0

#Checking Return order
def is_a_return(type):
   return 1 if type == 'RETURN' else 0
 
# Calculating Total Item  
def total_item_count(items):
    if items is not None:
        item_count =0
        for item in items:
            item_count = item_count + item['quantity']
        return item_count   

# Calculating Total Cost
def total_cost(items,type):
    if items is not None:
        total_cost =0
        item_price =0
    for item in items:
        item_price = (item['quantity']*item['unit_price'])
        total_cost = total_cost+ item_price
        item_price=0

    if type  == 'RETURN':
        return total_cost *-1
    else:
        return total_cost  


# Register UDFs
is_order = udf(is_a_order, IntegerType())
is_return = udf(is_a_return, IntegerType())
add_total_item_count = udf(total_item_count, IntegerType())
add_total_cost = udf(total_cost, FloatType())



# Reading input data from Kafka 
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","18.211.252.152:9092") \
    .option("subscribe","real-time-project") \
    .option("startingOffsets", "latest")  \
    .load()
        
        
# Define Schema
JSON_Schema = StructType() \
    .add("invoice_no", LongType()) \
    .add("country",StringType()) \
    .add("timestamp", TimestampType()) \
    .add("type", StringType()) \
    .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ])))

order_stream_data = raw_stream.select(from_json(col("value").cast("string"), JSON_Schema).alias("data")).select("data.*")

# Calculating additional columns for the stream 
order_stream_output = order_stream_data \
   .withColumn("total_cost", add_total_cost(order_stream_data.items,order_stream_data.type)) \
   .withColumn("total_items", add_total_item_count(order_stream_data.items)) \
   .withColumn("is_order", is_order(order_stream_data.type)) \
   .withColumn("is_return", is_return(order_stream_data.type))


# Writing the summarised input table to the console 
order_batch = order_stream_output \
   .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
   .writeStream \
   .outputMode("append") \
   .format("console") \
   .option("truncate", "false") \
   .option("path", "/Console_output") \
   .option("checkpointLocation", "/Console_output_checkpoints") \
   .trigger(processingTime="1 minute") \
   .start()
       
# Calculating Time based KPIs
agg_time = order_stream_output \
    .withWatermark("timestamp","1 minutes") \
    .groupby(window("timestamp", "1 minute")) \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        avg("total_cost").alias("average_transaction_size"),
        count("invoice_no").alias("OPM"),
        avg("is_Return").alias("rate_of_return")) \
    .select("window.start","window.end","OPM","total_volume_of_sales","average_transaction_size","rate_of_return")

# Calculating Time and country based KPIs
agg_time_country = order_stream_output \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(window("timestamp", "1 minutes"), "country") \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        count("invoice_no").alias("OPM"),
        avg("is_Return").alias("rate_of_return")) \
    .select("window.start","window.end","country", "OPM","total_volume_of_sales","rate_of_return")


# Writing to the Console : Time based KPI values 
ByTime = agg_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "timeKPIvalue") \
    .option("checkpointLocation", "timeKPIvalue_checkpoints") \
    .trigger(processingTime="1 minutes") \
    .start()


# Writing to the Console : Time and country based KPI values
ByTime_country = agg_time_country.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_countryKPIvalue") \
    .option("checkpointLocation", "time_countryKPIvalue_checkpoints") \
    .trigger(processingTime="1 minutes") \
    .start()

order_batch.awaitTermination()
ByTime.awaitTermination()
ByTime_country.awaitTermination()