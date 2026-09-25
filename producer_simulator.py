#!/usr/bin/env python3
"""
Streaming Producer Simulator (producer_simulator.py)
----------------------------------------------------
Simulates real-time e-commerce retail transactions by streaming rows from
OnlineRetail.csv into an Apache Kafka topic with dynamic timestamps and realistic
random delays.
"""

import os
import sys
import csv
import json
import time
import random
import argparse
import logging
from datetime import datetime, timezone
from configparser import ConfigParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PRODUCER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ProducerSimulator")





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
    """Load configuration from config.ini with environment variable fallbacks."""
    load_env_file(".env")
    config = ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)

    topic = os.getenv(
        "KAFKA_TOPIC",
        config.get("kafka", "topic", fallback="retail_stream")
    )
    kafka_port = os.getenv("KAFKA_PORT", "9092")
    default_servers = f"localhost:{kafka_port}"
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        config.get("kafka", "bootstrap_servers", fallback=default_servers)
    )
    csv_filepath = os.getenv(
        "CSV_FILEPATH",
        config.get("kafka", "csv_filepath", fallback="./data/OnlineRetail.csv")
    )

    return {
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "csv_filepath": csv_filepath,
    }


def ensure_dataset_exists(filepath: str):
    """Check if the dataset exists; if not, generate a realistic sample dataset."""
    if os.path.exists(filepath):
        return filepath

    # Check alternative common paths
    alternatives = ["./data/data.csv", "./OnlineRetail.csv", "data/OnlineRetail.csv"]
    for alt in alternatives:
        if os.path.exists(alt):
            logger.info(f"Found alternative dataset at: {alt}")
            return alt

    logger.warning(f"Dataset not found at '{filepath}'. Auto-generating sample retail dataset...")
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)

    sample_rows = [
        ["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "CustomerID", "Country"],
        ["536365", "85123A", "WHITE HANGING HEART T-LIGHT HOLDER", "6", "2026-09-24 08:26:00", "2.55", "17850", "United Kingdom"],
        ["536365", "71053", "WHITE METAL LANTERN", "6", "2026-09-24 08:26:00", "3.39", "17850", "United Kingdom"],
        ["536365", "84406B", "CREAM CUPID HEARTS COAT HANGER", "8", "2026-09-24 08:26:00", "2.75", "17850", "United Kingdom"],
        ["536365", "84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", "6", "2026-09-24 08:26:00", "3.39", "17850", "United Kingdom"],
        ["536366", "22633", "HAND WARMER UNION JACK", "32", "2026-09-24 08:28:00", "1.85", "17850", "United Kingdom"],
        ["C536379", "D", "Discount", "-1", "2026-09-24 09:41:00", "27.50", "14527", "United Kingdom"],
        ["536370", "22728", "ALARM CLOCK BAKELIKE PINK", "24", "2026-09-24 08:45:00", "3.75", "12583", "France"],
        ["536370", "22727", "ALARM CLOCK BAKELIKE RED", "24", "2026-09-24 08:45:00", "3.75", "12583", "France"],
        ["536370", "21724", "PANDA AND BUNNIES STICKER SHEET", "12", "2026-09-24 08:45:00", "0.85", "12583", "France"],
        ["536371", "22086", "PAPER CHAIN KIT 50'S CHRISTMAS", "80", "2026-09-24 09:00:00", "2.55", "13748", "United Kingdom"],
        ["536378", "22386", "JUMBO BAG PINK POLKADOT", "10", "2026-09-24 09:37:00", "1.95", "14688", "United Kingdom"],
        ["536381", "21523", "DOORMAT FANCY FONT HOME SWEET HOME", "10", "2026-09-24 09:41:00", "6.75", "15311", "United Kingdom"],
        ["536382", "22962", "JAM JAR WALL CLOCK", "4", "2026-09-24 09:45:00", "7.95", "16098", "United Kingdom"]
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(sample_rows)

    logger.info(f"Sample dataset successfully created at: {filepath}")
    return filepath


def create_kafka_producer(bootstrap_servers: str, max_retries: int = 10, retry_delay: int = 3):
    """Instantiate KafkaProducer with resilient connection retry logic."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error("kafka-python library not installed! Please run: pip install -r requirements.txt")
        sys.exit(1)

    producer = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to Kafka at '{bootstrap_servers}' (Attempt {attempt}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                request_timeout_ms=15000,
                api_version=(0, 10, 1),
            )
            logger.info("Successfully connected to Kafka broker!")
            return producer
        except Exception as e:
            if attempt < max_retries:
                logger.warning(f"Kafka broker not available yet ({e}). Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Could not connect to Kafka after {max_retries} attempts: {e}")
                raise
    return producer


def normalize_record(row: dict) -> dict:
    """Normalize row column keys and map to standard schema."""
    # Map common variations of column headers
    invoice = row.get("Invoice") or row.get("InvoiceNo") or row.get("invoice") or ""
    stock_code = row.get("StockCode") or row.get("stock_code") or ""
    description = row.get("Description") or row.get("description") or ""
    quantity = row.get("Quantity") or row.get("quantity") or "0"
    invoice_date = row.get("InvoiceDate") or row.get("invoice_date") or ""
    price = row.get("Price") or row.get("UnitPrice") or row.get("price") or "0.0"
    customer_id = row.get("CustomerID") or row.get("customer_id") or ""
    country = row.get("Country") or row.get("country") or "Unknown"

    # Current UTC timestamp when the event is emitted by the streaming simulator
    current_ts = datetime.now(timezone.utc).isoformat()

    return {
        "Invoice": str(invoice).strip(),
        "StockCode": str(stock_code).strip(),
        "Description": str(description).strip(),
        "Quantity": quantity,
        "InvoiceDate": str(invoice_date).strip(),
        "Price": price,
        "CustomerID": str(customer_id).strip(),
        "Country": str(country).strip(),
        "current_timestamp": current_ts,
    }


def stream_data(
    producer,
    topic: str,
    filepath: str,
    continuous_loop: bool = True,
    min_delay: float = 0.2,
    max_delay: float = 1.0,
    max_messages: int = 0,
):
    """Read CSV line-by-line and emit JSON payloads to Kafka with realistic delays."""
    total_sent = 0
    cycle = 1

    logger.info(f"Starting real-time streaming to topic '{topic}' from '{filepath}'")
    logger.info(f"Delay range: {min_delay}s - {max_delay}s | Continuous loop: {continuous_loop}")

    try:
        while True:
            logger.info(f"--- Streaming Cycle #{cycle} ---")
            with open(filepath, "r", encoding="utf-8", errors="replace") as csv_file:
                reader = csv.DictReader(csv_file)
                for row_idx, raw_row in enumerate(reader, start=1):
                    payload = normalize_record(raw_row)

                    # Send to Kafka topic
                    future = producer.send(topic, value=payload)
                    total_sent += 1

                    # Log message info
                    logger.info(
                        f"Sent #{total_sent:05d} | Topic: {topic} | "
                        f"Invoice: {payload['Invoice']} | "
                        f"Item: {payload['Description'][:28]}... | "
                        f"Qty: {payload['Quantity']} | Price: ${payload['Price']} | "
                        f"Country: {payload['Country']} | "
                        f"Timestamp: {payload['current_timestamp']}"
                    )

                    if max_messages > 0 and total_sent >= max_messages:
                        logger.info(f"Reached specified limit of {max_messages} messages.")
                        return

                    # Realistic delay between events (0.2 to 1.0 seconds as required)
                    delay = random.uniform(min_delay, max_delay)
                    time.sleep(delay)

            if not continuous_loop:
                logger.info("Reached end of dataset and loop mode is disabled. Finishing stream.")
                break

            cycle += 1
            logger.info("Dataset iteration completed. Looping dataset for continuous 24/7 streaming...")
            time.sleep(1.0)

    except KeyboardInterrupt:
        logger.info("\nStreaming interrupted by user (Ctrl+C). Flushing producer buffer...")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer closed cleanly. Total events streamed: {total_sent}")


def main():
    parser = argparse.ArgumentParser(description="Streaming Producer Simulator for Apache Kafka")
    parser.add_argument("--config", default="config.ini", help="Path to config.ini file")
    parser.add_argument("--topic", default=None, help="Kafka topic name")
    parser.add_argument("--bootstrap-servers", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--file", default=None, help="Path to CSV dataset")
    parser.add_argument("--min-delay", type=float, default=0.2, help="Minimum delay between messages in seconds")
    parser.add_argument("--max-delay", type=float, default=1.0, help="Maximum delay between messages in seconds")
    parser.add_argument("--max-messages", type=int, default=0, help="Limit number of messages (0 for unlimited)")
    parser.add_argument("--no-loop", action="store_true", help="Do not loop dataset indefinitely")

    args = parser.parse_args()

    # Load configuration
    cfg = load_configurations(args.config)
    topic = args.topic or cfg["topic"]
    bootstrap_servers = args.bootstrap_servers or cfg["bootstrap_servers"]
    csv_filepath = args.file or cfg["csv_filepath"]

    # Verify or generate dataset
    resolved_filepath = ensure_dataset_exists(csv_filepath)

    # Initialize Kafka Producer
    producer = create_kafka_producer(bootstrap_servers)

    # Start Streaming
    stream_data(
        producer=producer,
        topic=topic,
        filepath=resolved_filepath,
        continuous_loop=not args.no_loop,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_messages=args.max_messages,
    )


if __name__ == "__main__":
    main()
