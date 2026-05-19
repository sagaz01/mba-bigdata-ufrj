from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pathlib import Path

# Configuração de diretórios usando Path
BASE_DIR = Path("datalake")
(BASE_DIR / "silver_processed").mkdir(parents=True, exist_ok=True)
(BASE_DIR / "gold_anomalies").mkdir(parents=True, exist_ok=True)

spark = SparkSession.builder \
    .appName("SeismicSubsurfaceMapping_UFRJ") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

seismic_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("sensor_node", StringType(), True),
    StructField("target_site", StringType(), True),
    StructField("basin_region", StringType(), True),
    StructField("acoustic_amplitude_db", DoubleType(), True),
    StructField("signal_noise_ratio", DoubleType(), True)
])

def run_streaming_pipeline():
    print(">>> Motor Spark Structured Streaming iniciado. Aguardando dados...")
    
    # Ingestão (Bronze Layer)
    raw_stream = spark.readStream \
        .schema(seismic_schema) \
        .option("header", "true") \
        .option("maxFilesPerTrigger", 1) \
        .csv("datalake/raw_seismic/*.csv")
    
    # Cast de data
    stream_df = raw_stream.withColumn("event_time", F.col("event_time").cast(TimestampType()))
    
    # Transformação (Silver Layer) - Agrupamento em janelas de 30 segundos
    silver_df = stream_df \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(
            F.window(F.col("event_time"), "30 seconds"),
            F.col("sensor_node"), F.col("target_site"), F.col("basin_region")
        ) \
        .agg(
            F.avg("acoustic_amplitude_db").alias("mean_amplitude"),
            F.max("acoustic_amplitude_db").alias("peak_amplitude"), # Novo indicador adicionado!
            F.avg("signal_noise_ratio").alias("mean_snr")
        ).withColumn("etl_timestamp", F.current_timestamp())
    
    # Filtragem de Alto Potencial (Gold Layer) - Onde o sinal é muito forte
    gold_df = silver_df.filter((F.col("mean_amplitude") >= 70) & (F.col("mean_snr") >= 20))
    
    # Escrita no disco
    write_silver = silver_df.writeStream.outputMode("append").format("parquet") \
        .option("path", "datalake/silver_processed") \
        .option("checkpointLocation", "checkpoints/silver").start()
        
    write_gold = gold_df.writeStream.outputMode("append").format("parquet") \
        .option("path", "datalake/gold_anomalies") \
        .option("checkpointLocation", "checkpoints/gold").start()
    
    write_silver.awaitTermination()

if __name__ == "__main__":
    run_streaming_pipeline()