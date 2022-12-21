from os.path import abspath

from pyspark.sql import SparkSession


warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Exchange rates processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file exchange_rates.json from the HDFS
df = spark.read.json('hdfs://namenode:9000/exchange_rates/exchange_rates.json')

# Drop the duplicated rows based on the base and last_update columns
exchange_rates = df.select('base', 'last_update', 'rates.eur', 'rates.usd', 'rates.aud', 'rates.brl', 'rates.cad', 'rates.cop', 'rates.dkk', 'rates.egp') \
    .dropDuplicates(['base', 'last_update']) \
    .fillna(1, subset=['EUR', 'USD', 'AUD', 'BRL', 'CAD', 'COP', 'DKK', 'EGP'])

# Export the dataframe into the Hive table exchange_rates
exchange_rates.write.mode("append").insertInto("exchange_rates")