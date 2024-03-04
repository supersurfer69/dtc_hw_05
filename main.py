from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql import SparkSession

input_path = './fhv_tripdata_2019-10.csv'
output_path = ''

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropOff_datetime', types.TimestampType(), True),
    types.StructField('PUlocationID', types.IntegerType(), True),
    types.StructField('DOlocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.IntegerType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)])

df = spark.read.option('header', 'true').schema(schema).csv(input_path)

# 2nd question
df.repartition(6)
df.write.parquet('/Users/romamerz/fhvhv/2019/10/')

# 3rd question
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropOff_date', F.to_date(df.dropOff_datetime)) \
    .filter(F.col('pickup_date') == '2019-10-15') \
    .count()

# 4th question
df \
    .withColumn('pickup_ts', F.col('pickup_datetime').cast('long')) \
    .withColumn('dropoff_ts', F.col('dropOff_datetime').cast('long')) \
    .withColumn('trip_duration', (F.col('dropoff_ts') - F.col('pickup_ts')) / 3600) \
    .agg(F.max('trip_duration')).show()

# 6th question
schema_lookup = types.StructType(
    [types.StructField('LocationID', types.IntegerType(), True),
     types.StructField('Borough', types.StringType(), True),
     types.StructField('Zone', types.StringType(), True),
     types.StructField('service_zone', types.StringType(), True)]
)
df_lookup = spark.read.option('header', 'true').schema(schema_lookup).csv('taxi_zone_lookup.csv')
df.join(df_lookup, df.PUlocationID == df_lookup.LocationID, how='left').registerTempTable('trips_with_zones')
spark.sql('select Zone, count(*) as cnt from trips_with_zones group by Zone order by cnt').show()
