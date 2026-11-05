from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *





EH_NAMESPACE = "ubereventd"
EH_NAME = "ubertopic"

EH_CONN_STR = "Endpoint=sb://ubereventd.servicebus.windows.net/;SharedAccessKeyName=ListenPolicy;SharedAccessKey=<ACCESS_KEY>;EntityPath=ubertopic"

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'True',
  "startingOffsets"          : 'earliest'
}


@dp.table
def rides_raw():
  df = spark.readStream.format("kafka")\
      .options(**KAFKA_OPTIONS)\
      .load()

  df = df.withColumn("rides",col("value").cast("string"))
  return df

