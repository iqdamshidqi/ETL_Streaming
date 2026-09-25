#!/usr/bin/env python3
"""
PySpark Structured Streaming Consumer (spark_streaming.py)
----------------------------------------------------------
Reads real-time e-commerce transactions from Apache Kafka,
cleans, parses, and validates the streaming schema, and continuously
persists micro-batches into PostgreSQL using JDBC and checkpointing.
"""

import os
import sys
import time
import logging
from configparser import ConfigParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SPARK-STREAM] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkStreamingConsumer")

# Try initializing findspark if installed
try:
    import findspark
    findspark.init()
except Exception:
    pass

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        DoubleType,
        TimestampType,
        BooleanType,
    )
except ImportError:
    logger.error("PySpark library not found! Install it with: pip install pyspark")
    sys.exit(1)


def load_env_file(env_path: str = ".env"):
    """Load key-value pairs from .env into os.environ if present."""
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    key, val = key.strip(), val.strip().strip("'\"")
                    if key not in os.environ:
                        os.environ[key] = val


def load_configurations(config_path: str = "config.ini"):
    """Load streaming pipeline configuration from file and environment variables."""
    load_env_file(".env")
    config = ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)

    # Kafka configuration
    kafka_topic = os.getenv(
        "KAFKA_TOPIC",
        config.get("kafka", "topic", fallback="retail_stream")
    )
    kafka_port = os.getenv("KAFKA_PORT", "9092")
    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        config.get("kafka", "bootstrap_servers", fallback=f"localhost:{kafka_port}")
    )

    # PostgreSQL configuration
    postgres_host = os.getenv(
        "POSTGRES_HOST",
        config.get("postgresql", "postgresql_host", fallback="localhost")
    )
    postgres_port = os.getenv(
        "POSTGRES_PORT",
        config.get("postgresql", "postgresql_port", fallback="5432")
    )
    postgres_db = os.getenv(
        "POSTGRES_DB",
        config.get("postgresql", "postgresql_database", fallback="retail_db")
    )
    postgres_table = os.getenv(
        "POSTGRES_TABLE",
        config.get("postgresql", "postgresql_table", fallback="retail_transactions")
    )
    postgres_user = os.getenv(
        "POSTGRES_USER",
        config.get("postgresql", "postgresql_user", fallback="postgres")
    )
    postgres_password = os.getenv(
        "POSTGRES_PASSWORD",
        config.get("postgresql", "postgresql_pwd", fallback="postgrespassword")
    )
    postgres_driver = os.getenv(
        "POSTGRES_DRIVER",
        config.get("postgresql", "postgresql_driver", fallback="org.postgresql.Driver")
    )
    postgres_url = os.getenv(
        "POSTGRES_URL",
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    )

    # Spark streaming configuration
    app_name = os.getenv(
        "SPARK_APP_NAME",
        config.get("spark", "app_name", fallback="RetailRealTimeStreamingPipeline")
    )
    checkpoint_dir = os.getenv(
        "SPARK_CHECKPOINT_DIR",
        config.get("spark", "checkpoint_dir", fallback="temp/checkpoints/retail_stream")
    )
    trigger_time = os.getenv(
        "TRIGGER_TIME",
        config.get("spark", "trigger_processing_time", fallback="2 seconds")
    )

    return {
        "kafka_topic": kafka_topic,
        "kafka_bootstrap_servers": kafka_bootstrap_servers,
        "postgres_url": postgres_url,
        "postgres_table": postgres_table,
        "postgres_user": postgres_user,
        "postgres_password": postgres_password,
        "postgres_driver": postgres_driver,
        "app_name": app_name,
        "checkpoint_dir": checkpoint_dir,
        "trigger_time": trigger_time,
    }


def get_spark_session(app_name: str) -> SparkSession:
    """Create and configure SparkSession with required Kafka & Postgres dependencies."""
    logger.info("Initializing SparkSession for Continuous Real-Time Streaming...")

    # Allow custom packages override via environment variable
    packages = os.getenv("SPARK_PACKAGES")
    if not packages:
        # Detect PySpark version to choose Scala 2.12 vs 2.13
        try:
            import pyspark
            major_ver = int(pyspark.__version__.split(".")[0])
            scala_ver = "2.13" if major_ver >= 4 else "2.12"
            spark_pkg_ver = "3.5.3" if major_ver >= 4 else "3.5.0"
        except Exception:
            scala_ver = "2.12"
            spark_pkg_ver = "3.5.0"

        kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_ver}:{spark_pkg_ver}"
        postgres_pkg = "org.postgresql:postgresql:42.6.0"
        packages = f"{kafka_pkg},{postgres_pkg}"

    logger.info(f"Using Spark packages: {packages}")

    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "512m")
    executor_mem = os.getenv("SPARK_EXECUTOR_MEMORY", "512m")

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", driver_mem) \
        .config("spark.executor.memory", executor_mem) \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()


    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized successfully.")
    return spark


def build_retail_schema() -> StructType:
    """Define structured schema for parsing incoming JSON payload from Kafka."""
    return StructType([
        StructField("Invoice", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", StringType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("Price", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("current_timestamp", StringType(), True),
    ])


def create_postgres_writer(postgres_url: str, postgres_table: str, postgres_user: str, postgres_password: str, postgres_driver: str):
    """Factory creating a foreachBatch writing function with logging and error handling."""
    def write_to_postgres(batch_df, batch_id):
        record_count = batch_df.count()
        if record_count == 0:
            return

        print("\n" + "=" * 76)
        print(f"  [STREAMING INGESTION] Micro-Batch #{batch_id} | Records: {record_count}")
        print(f"  Batch Timestamp : {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Target Database : {postgres_url} -> Table: {postgres_table}")
        print("=" * 76)

        # Display preview of transformed batch in terminal
        batch_df.select(
            "invoice", "stock_code", "description", "quantity",
            "price", "total_amount", "is_cancelled", "country", "processed_at"
        ).show(min(5, record_count), truncate=False)

        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", postgres_table) \
                .option("user", postgres_user) \
                .option("password", postgres_password) \
                .option("driver", postgres_driver) \
                .mode("append") \
                .save()
            print(f"  >>> SUCCESS: Ingested {record_count} records into PostgreSQL '{postgres_table}'.\n")
        except Exception as err:
            logger.error(f"Failed to persist micro-batch #{batch_id} to PostgreSQL: {err}", exc_info=True)

    return write_to_postgres


def main():
    config = load_configurations()

    logger.info("==================================================")
    logger.info("Starting Pure Real-Time Streaming Consumer Pipeline")
    logger.info("==================================================")
    logger.info(f"Kafka Broker      : {config['kafka_bootstrap_servers']}")
    logger.info(f"Kafka Topic       : {config['kafka_topic']}")
    logger.info(f"PostgreSQL Target : {config['postgres_url']} (Table: {config['postgres_table']})")
    logger.info(f"Checkpoint Dir    : {config['checkpoint_dir']}")
    logger.info(f"Trigger Interval  : {config['trigger_time']}")

    # 1. Initialize Spark
    spark = get_spark_session(config["app_name"])

    # 2. Define structured JSON Schema
    retail_schema = build_retail_schema()

    # 3. Read stream directly from Kafka topic
    logger.info(f"Subscribing to Kafka topic: {config['kafka_topic']}...")
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"]) \
        .option("subscribe", config["kafka_topic"]) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 4. Parse JSON payload
    parsed_stream = kafka_stream.select(
        F.from_json(F.col("value").cast("string"), retail_schema).alias("payload"),
        F.col("timestamp").alias("kafka_timestamp")
    ).select("payload.*", "kafka_timestamp")

    # 5. Clean, format, and enrich streaming columns
    cleaned_stream = parsed_stream \
        .filter(F.col("Invoice").isNotNull() & (F.trim(F.col("Invoice")) != "")) \
        .withColumn("invoice", F.trim(F.col("Invoice"))) \
        .withColumn("stock_code", F.trim(F.col("StockCode"))) \
        .withColumn("description",
            F.when(F.col("Description").isNull() | (F.trim(F.col("Description")) == ""), "Description Not Provided")
             .otherwise(F.trim(F.col("Description")))
        ) \
        .withColumn("quantity", F.coalesce(F.expr("try_cast(Quantity AS INT)"), F.lit(0))) \
        .withColumn("price", F.coalesce(F.expr("try_cast(Price AS DOUBLE)"), F.lit(0.0))) \
        .withColumn("customer_id",
            F.when(
                F.col("CustomerID").isNull() |
                (F.trim(F.col("CustomerID")) == "") |
                (F.col("CustomerID") == "nan"),
                "Unknown"
            ).otherwise(F.trim(F.col("CustomerID")))
        ) \
        .withColumn("country",
            F.when(F.col("Country").isNull() | (F.trim(F.col("Country")) == ""), "Unknown")
             .otherwise(F.trim(F.col("Country")))
        ) \
        .withColumn("is_cancelled", F.col("Invoice").rlike("^[cC]")) \
        .withColumn("total_amount", F.round(F.abs(F.col("quantity")) * F.col("price"), 2)) \
        .withColumn("invoice_date",
            F.coalesce(
                F.to_timestamp(F.col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("InvoiceDate"), "dd/MM/yyyy HH:mm"),
                F.to_timestamp(F.col("InvoiceDate")),
                F.current_timestamp()
            )
        ) \
        .withColumn("event_timestamp",
            F.coalesce(
                F.to_timestamp(F.col("current_timestamp")),
                F.current_timestamp()
            )
        ) \
        .withColumn("processed_at", F.current_timestamp()) \
        .select(
            "invoice",
            "stock_code",
            "description",
            "quantity",
            "invoice_date",
            "price",
            "customer_id",
            "country",
            "is_cancelled",
            "total_amount",
            "event_timestamp",
            "processed_at"
        )

    # 6. Build sink writer for PostgreSQL
    write_to_postgres = create_postgres_writer(
        postgres_url=config["postgres_url"],
        postgres_table=config["postgres_table"],
        postgres_user=config["postgres_user"],
        postgres_password=config["postgres_password"],
        postgres_driver=config["postgres_driver"]
    )

    # Ensure checkpoint directory exists
    os.makedirs(config["checkpoint_dir"], exist_ok=True)

    # 7. Start Structured Streaming query with foreachBatch & checkpointing
    streaming_query = cleaned_stream.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .trigger(processingTime=config["trigger_time"]) \
        .option("checkpointLocation", config["checkpoint_dir"]) \
        .start()

    logger.info("==========================================================")
    logger.info("Pipeline is actively streaming 24/7. Waiting for events...")
    logger.info("Press Ctrl+C to stop.")
    logger.info("==========================================================")

    # 8. Keep streaming continuously 24/7
    try:
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received termination signal. Stopping streaming query...")
        streaming_query.stop()
        logger.info("Streaming query stopped successfully.")


if __name__ == "__main__":
    main()
