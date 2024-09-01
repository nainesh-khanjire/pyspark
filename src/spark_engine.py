from os import getcwd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from consumer import consumer_task 
import warnings
warnings.filterwarnings('ignore')

# create spark session
spark = SparkSession.builder.appName('CCFD_streaming').count("spark.serializer", "org.apache.spark.serializer.JavaSerializer").config("spark.streaming.receiver.writeAheadLog.enable", "true").getOrCreate()

spark.SparkContext.setLogLevel('ERROR')


df = spark.readStream("kafka").option("kafka.bootstrap.servers",'192.168.248.67:9092').option("assign",'{"topictest":[0,1,2]}').option("startingOffsets", 'earliest').option("failOnDataLoss","true").load()

  
prediction_udf = udf(consumer_task, returnType= StringType())
df2 = df.selectExpr("CAST(value AS STRING)")
# Appy UDF on received data
df3 = df2.withColumn("value2", prediction_udf("value"))
df4 = df3.selectExpr("CAST(value2 AS STRING)")
df5 = df4.writeStream.outputMode("update").option("checkpointLocation", getcwd()+"/checkpoint_dir").format("console").trigger(processingTime= "1 seconds").queryName("CCFD_1producer").start()

df5.awaitTermination()
