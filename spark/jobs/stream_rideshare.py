import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def main():
    print("[Spark] Starting PySpark Bronze Streaming Job...", flush=True)
    
    # 1. Initialize Spark Session
    # Spark defaults in spark-defaults.conf will automatically load GCS connector class names.
    spark = SparkSession.builder \
        .appName("BostonRideshareBronzeStreaming") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # 2. Get environment variables
    gcp_project_id = os.environ.get("GCP_PROJECT_ID", "boston-rideshare-analytics")
    gcs_bucket = os.environ.get("GCS_BUCKET", "boston-rideshare-data")
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "rideshare_events")
    
    # Check if Service Account JSON key exists to enable GCS vs Local output paths
    key_path = "/opt/gcp/service-account.json"
    use_gcs = os.path.exists(key_path) and gcp_project_id != "your-gcp-project-id"
    
    if use_gcs:
        print(f"[Spark] GCP Service Account found. Authenticating for GCS bucket: {gcs_bucket} in project: {gcp_project_id}", flush=True)
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.project.id", gcp_project_id)
        bronze_path = f"gs://{gcs_bucket}/bronze/rideshare_events"
        checkpoint_bronze = f"gs://{gcs_bucket}/bronze/checkpoint_rideshare_events"
    else:
        print("[Spark] No GCP Service Account found or default project ID active. Streaming to local filesystem for testing.", flush=True)
        bronze_path = "/opt/spark/data/bronze/rideshare_events"
        checkpoint_bronze = "/opt/spark/data/bronze/checkpoint_rideshare_events"
        
    print(f"[Spark] Source Topic: {topic}")
    print(f"[Spark] Kafka Broker: {kafka_servers}")
    print(f"[Spark] Bronze Path : {bronze_path}")
    
    # 3. Read stream from Kafka
    try:
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
    except Exception as e:
        print(f"[Spark] Error initializing Kafka readStream: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
        
    # 4. Bronze Layer: Raw Kafka Ingestion Archive (JSON string + metadata)
    bronze_df = kafka_stream \
        .selectExpr(
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value",
            "partition",
            "offset",
            "timestamp as kafka_timestamp"
        ) \
        .withColumn("ingestion_time", current_timestamp())
        
    # 5. Start Write Stream for Bronze Layer
    print("[Spark] Starting Bronze stream writer...", flush=True)
    query_bronze = bronze_df.writeStream \
        .format("parquet") \
        .option("path", bronze_path) \
        .option("checkpointLocation", checkpoint_bronze) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
        
    print("[Spark] Bronze stream successfully initialized. Awaiting termination...", flush=True)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
