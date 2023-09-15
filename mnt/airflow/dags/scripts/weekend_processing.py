from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, from_unixtime
from pyspark.sql.functions import from_utc_timestamp

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Weekend processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file weekday_data.json from the HDFS
df = spark.read.json('hdfs://namenode:9000/weekend/weekend_data.json')

# Convert the 'datetime' field from Unix timestamp to a timestamp datatype
df = df.withColumn('datetime', from_unixtime(df['datetime'] / 1000).cast('timestamp'))

# Convert the 'datetime' field to UTC
df = df.withColumn('datetime', from_utc_timestamp(df['datetime'], 'UTC'))

# Drop the duplicated rows based on the datetime and street_id columns
weekend_data = df.select('datetime', 'street_id', 'count', 'vel') \

# Assuming you have a Hive table named 'weekday_traffic'
#spark.sql("TRUNCATE TABLE weekday_traffic")

# Now, you can proceed to write the new data into the table
weekend_data.write.mode("append").insertInto("weekend_traffic")
