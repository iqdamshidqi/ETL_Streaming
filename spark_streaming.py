#!/usr/bin/env python3
"""
================================================================================
PYSPARK STRUCTURED STREAMING CONSUMER (spark_streaming.py)
================================================================================
Modul ini bertindak sebagai "Mesin Pengolah Data / Dapur Cepat" dalam arsitektur
data streaming. 

Alur Kerja:
1. Menghubungkan PySpark ke Apache Kafka topic 'retail_stream' (Streaming Source).
2. Membaca aliran data biner JSON secara terus menerus (Unbounded Table).
3. Mengurai (parse) JSON sesuai skema ketat (Schema Enforcement).
4. Melakukan pembersihan data (Cleansing): penanganan nilai NULL, pemangkasan spasi.
5. Melakukan pengayaan data (Enrichment):
   - Deteksi pesanan retur / batal (is_cancelled).
   - Perhitungan total nominal belanja (total_amount = quantity * price).
   - Penyelarasan format tanggal transaksi (invoice_date) & penanda waktu proses (processed_at).
6. Menyimpan hasil micro-batch ke database PostgreSQL via JDBC (Sink) dengan
   dukungan checkpointing untuk ketahanan terhadap kegagalan (Fault Tolerance).
================================================================================
"""

# ==============================================================================
# 1. IMPORT MODUL & PUSTAKA STANDAR
# ==============================================================================
import os                  # Mengakses file sistem dan environment variables
import sys                 # Operasi sistem interpreter (misal keluar dari program saat error)
import time                # Pencatatan stempel waktu (timestamp) pada batch
import logging             # Logging aktivitas streaming di konsol
from configparser import ConfigParser  # Membaca konfigurasi dari config.ini

# Mengonfigurasi tampilan log terminal agar jelas bagi siswa saat membaca output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SPARK-STREAM] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkStreamingConsumer")

# Inisialisasi findspark (jika ada lingkungan lokal yang membutuhkan pencarian SPARK_HOME)
try:
    import findspark
    findspark.init()
except Exception:
    pass

# ==============================================================================
# 2. IMPORT KOMPONEN PYSPARK SQL & TIPE DATA
# ==============================================================================
try:
    from pyspark.sql import SparkSession                # Objek utama pengendali komputasi Spark
    from pyspark.sql import functions as F              # Koleksi fungsi manipulasi kolom bawaan Spark
    from pyspark.sql.types import (                     # Tipe data untuk pembuatan skema tabel
        StructType,                                     # Merepresentasikan skema sebuah tabel / objek struct
        StructField,                                    # Merepresentasikan sebuah kolom dalam StructType
        StringType,                                     # Tipe data teks / karakter
        IntegerType,                                    # Tipe data bilangan bulat (32-bit)
        DoubleType,                                     # Tipe data bilangan pecahan presisi ganda
        TimestampType,                                  # Tipe data tanggal dan waktu (datetime)
        BooleanType,                                    # Tipe data boolean (True/False)
    )
except ImportError:
    logger.error("Pustaka PySpark tidak ditemukan! Silakan pasang dengan: pip install pyspark")
    sys.exit(1)


# ==============================================================================
# 3. FUNGSI PEMBACA ENVIRONMENT VARIABLE (.env)
# ==============================================================================
def load_env_file(env_path: str = ".env"):
    """
    Membaca berkas .env untuk memungkinkan kustomisasi port dan kredensial database
    tanpa perlu mengubah kode program Python.
    """
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    key, val = key.strip(), val.strip().strip("'\"")
                    if key not in os.environ:
                        os.environ[key] = val


# ==============================================================================
# 4. FUNGSI PEMUAT KONFIGURASI PIPELINE
# ==============================================================================
def load_configurations(config_path: str = "config.ini"):
    """
    Memuat seluruh parameter koneksi (Kafka, PostgreSQL, Spark) dari file config.ini
    dengan fallback otomatis ke variabel lingkungan (.env).
    """
    load_env_file(".env")
    config = ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)

    # --- Parameter Apache Kafka ---
    kafka_topic = os.getenv(
        "KAFKA_TOPIC",
        config.get("kafka", "topic", fallback="retail_stream")
    )
    kafka_port = os.getenv("KAFKA_PORT", "9092")
    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        config.get("kafka", "bootstrap_servers", fallback=f"localhost:{kafka_port}")
    )

    # --- Parameter Database PostgreSQL ---
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
    # URL JDBC untuk membuka koneksi ke PostgreSQL
    postgres_url = os.getenv(
        "POSTGRES_URL",
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    )

    # --- Parameter PySpark Streaming ---
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


# ==============================================================================
# 5. FUNGSI INISIALISASI SPARK SESSION DENGAN OPTIMASI MEMORI
# ==============================================================================
def get_spark_session(app_name: str) -> SparkSession:
    """
    Membangun sesi Spark (SparkSession) dengan mengunduh paket dependensi Maven
    (Konektor Kafka dan Driver PostgreSQL JDBC) serta mengoptimasi performa untuk mesin lokal.
    
    Penjelasan Konfigurasi Kunci:
    - master("local[*]"): Memanfaatkan semua core CPU mesin lokal secara paralel.
    - spark.jars.packages: Mengaitkan library JAR Kafka-SQL dan PostgreSQL driver.
    - spark.sql.shuffle.partitions = "2": 
      SANGAT PENTING: Nilai bawaan Spark adalah 200 partisi. Pada laptop siswa, 200 partisi
      akan menciptakan overhead thread yang membuat laptop lemot. Diturunkan menjadi 2 agar ringan.
    - spark.sql.ansi.enabled = "false":
      Mencegah Spark langsung mematikan aplikasi saat terjadi error konversi tipe data;
      menggantinya dengan nilai NULL yang aman ditangani kemudian.
    """
    logger.info("Menginisialisasi SparkSession untuk Pemrosesan Streaming Berkelanjutan...")

    # Cek apakah ada paket pustaka kustom dari environment variable
    packages = os.getenv("SPARK_PACKAGES")
    if not packages:
        # Menentukan versi Scala yang sesuai dengan versi PySpark yang terpasang
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

    logger.info(f"Paket JAR yang digunakan: {packages}")

    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "512m")
    executor_mem = os.getenv("SPARK_EXECUTOR_MEMORY", "512m")

    # Membangun SparkSession
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

    # Redam log internal Spark yang berisik, hanya tampilkan peringatan (WARN) dan error
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession berhasil diinisialisasi.")
    return spark


# ==============================================================================
# 6. DEFINISI SKEMA DATA TRANSAKSI RETAIL (SCHEMA ENFORCEMENT)
# ==============================================================================
def build_retail_schema() -> StructType:
    """
    Mendefinisikan skema terstruktur untuk mem-parsing payload JSON dari Kafka.
    
    Penjelasan Desain:
    Mengapa kolom angka (Quantity, Price) didefinisikan sebagai StringType di awal?
    Ini adalah teknik "Defensive Ingestion". Jika kolom angka langsung dipaksa menjadi
    Integer/Double saat parsing JSON mentah, data yang kotor (seperti ada karakter aneh)
    akan membuat seluruh record hangus/NULL. Dengan membacanya sebagai StringType dahulu,
    kita dapat melakukan casting terkontrol menggunakan try_cast pada tahap transformasi.
    """
    return StructType([
        StructField("Invoice", StringType(), True),            # Nomor nota transaksi
        StructField("StockCode", StringType(), True),          # Kode unik barang
        StructField("Description", StringType(), True),        # Keterangan nama barang
        StructField("Quantity", StringType(), True),           # Kuantitas unit belanja
        StructField("InvoiceDate", StringType(), True),        # Tanggal nota dari transaksi asli
        StructField("Price", StringType(), True),              # Harga per unit barang
        StructField("CustomerID", StringType(), True),         # Identitas pembeli
        StructField("Country", StringType(), True),            # Negara asal transaksi
        StructField("current_timestamp", StringType(), True),  # Stempel waktu kirim dari producer
    ])


# ==============================================================================
# 7. WRITER FUNCTION KE POSTGRESQL MELALUI FOREACHBATCH
# ==============================================================================
def create_postgres_writer(postgres_url: str, postgres_table: str, postgres_user: str, postgres_password: str, postgres_driver: str):
    """
    Factory function yang menghasilkan fungsi callback 'write_to_postgres' untuk
    digunakan oleh .foreachBatch().
    
    Mengapa menggunakan foreachBatch?
    Secara native, Spark Streaming tidak memiliki sink bawaan langsung ke JDBC. 
    Dengan foreachBatch, Spark membagi stream menjadi DataFrame micro-batch kecil
    setiap 2 detik, lalu mengeksekusi penulisan standar batch_df.write.format('jdbc')
    yang sangat stabil dan kompatibel dengan PostgreSQL.
    """
    def write_to_postgres(batch_df, batch_id):
        # Hitung jumlah record di dalam micro-batch saat ini
        record_count = batch_df.count()
        if record_count == 0:
            return  # Jika tidak ada data baru masuk di batch ini, lewati

        # Cetak banner visual di terminal untuk mempermudah monitoring siswa
        print("\n" + "=" * 76)
        print(f"  [STREAMING INGESTION] Micro-Batch #{batch_id} | Jumlah Data: {record_count} baris")
        print(f"  Waktu Eksekusi   : {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Tujuan Database  : {postgres_url} -> Tabel: {postgres_table}")
        print("=" * 76)

        # Tampilkan pratinjau 5 baris pertama data yang sudah ditransformasi di terminal
        batch_df.select(
            "invoice", "stock_code", "description", "quantity",
            "price", "total_amount", "is_cancelled", "country", "processed_at"
        ).show(min(5, record_count), truncate=False)

        try:
            # Menyimpan DataFrame ke tabel PostgreSQL menggunakan konektor JDBC
            batch_df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", postgres_table) \
                .option("user", postgres_user) \
                .option("password", postgres_password) \
                .option("driver", postgres_driver) \
                .mode("append") \
                .save()
            print(f"  >>> SUKSES: Berhasil menyimpan {record_count} baris ke PostgreSQL '{postgres_table}'.\n")
        except Exception as err:
            logger.error(f"Gagal menyimpan micro-batch #{batch_id} ke PostgreSQL: {err}", exc_info=True)

    return write_to_postgres


# ==============================================================================
# 8. ALUR LOGIKA UTAMA (MAIN PIPELINE FUNCTION)
# ==============================================================================
def main():
    # 1. Pemuatan konfigurasi
    config = load_configurations()

    logger.info("==================================================")
    logger.info("Memulai Pure Real-Time Streaming Consumer Pipeline")
    logger.info("==================================================")
    logger.info(f"Kafka Broker      : {config['kafka_bootstrap_servers']}")
    logger.info(f"Kafka Topic       : {config['kafka_topic']}")
    logger.info(f"Target Database   : {config['postgres_url']} (Tabel: {config['postgres_table']})")
    logger.info(f"Direktori Checkpoint: {config['checkpoint_dir']}")
    logger.info(f"Interval Trigger  : {config['trigger_time']}")

    # 2. Buat SparkSession
    spark = get_spark_session(config["app_name"])

    # 3. Buat skema struct untuk parsing JSON
    retail_schema = build_retail_schema()

    # 4. Berlangganan (Subscribe) ke aliran Kafka topic
    logger.info(f"Mulai berlangganan ke Kafka topic: {config['kafka_topic']}...")
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"]) \
        .option("subscribe", config["kafka_topic"]) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 5. Parsing payload JSON dari kolom 'value' (berformat biner)
    parsed_stream = kafka_stream.select(
        # Mengubah byte biner ke teks string, lalu parsing menggunakan retail_schema
        F.from_json(F.col("value").cast("string"), retail_schema).alias("payload"),
        # Menyimpan timestamp kedatangan pesan di Kafka broker
        F.col("timestamp").alias("kafka_timestamp")
    ).select("payload.*", "kafka_timestamp")  # Ekstrak seluruh field JSON menjadi kolom mandiri

    # 6. Pembersihan Data (Cleansing) dan Pengayaan Data Bisnis (Enrichment)
    cleaned_stream = parsed_stream \
        .filter(F.col("Invoice").isNotNull() & (F.trim(F.col("Invoice")) != "")) \
        .withColumn("invoice", F.trim(F.col("Invoice"))) \
        .withColumn("stock_code", F.trim(F.col("StockCode"))) \
        .withColumn("description",
            # Jika deskripsi kosong / NULL, berikan teks pengganti default
            F.when(F.col("Description").isNull() | (F.trim(F.col("Description")) == ""), "Description Not Provided")
             .otherwise(F.trim(F.col("Description")))
        ) \
        .withColumn("quantity", 
            # try_cast aman: jika data kotor/bukan angka, menghasilkan NULL lalu diubah jadi 0
            F.coalesce(F.expr("try_cast(Quantity AS INT)"), F.lit(0))
        ) \
        .withColumn("price", 
            # try_cast aman untuk harga satuan belanja
            F.coalesce(F.expr("try_cast(Price AS DOUBLE)"), F.lit(0.0))
        ) \
        .withColumn("customer_id",
            # Standarisasi nilai pembeli yang hilang / anonim
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
        .withColumn("is_cancelled", 
            # Regex: Menandai transaksi batal jika nomor faktur diawali huruf 'C' atau 'c'
            F.col("Invoice").rlike("^[cC]")
        ) \
        .withColumn("total_amount", 
            # Nilai total omset belanja: pembulatan 2 desimal dari |quantity| * price
            F.round(F.abs(F.col("quantity")) * F.col("price"), 2)
        ) \
        .withColumn("invoice_date",
            # Parsing tanggal multi-format yang adaptif terhadap berbagai gaya penulisan tanggal
            F.coalesce(
                F.to_timestamp(F.col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("InvoiceDate"), "dd/MM/yyyy HH:mm"),
                F.to_timestamp(F.col("InvoiceDate")),
                F.current_timestamp()
            )
        ) \
        .withColumn("event_timestamp",
            # Waktu transaksi dipancarkan oleh simulator producer
            F.coalesce(
                F.to_timestamp(F.col("current_timestamp")),
                F.current_timestamp()
            )
        ) \
        .withColumn("processed_at", F.current_timestamp()) \
        .select(
            # Susun urutan kolom yang sesuai dengan skema tabel di PostgreSQL
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

    # 7. Mempersiapkan fungsi writer ke database PostgreSQL
    write_to_postgres = create_postgres_writer(
        postgres_url=config["postgres_url"],
        postgres_table=config["postgres_table"],
        postgres_user=config["postgres_user"],
        postgres_password=config["postgres_password"],
        postgres_driver=config["postgres_driver"]
    )

    # Pastikan folder checkpoint tersedia di disk lokal
    os.makedirs(config["checkpoint_dir"], exist_ok=True)

    # 8. Memulai Streaming Query dengan foreachBatch dan Checkpoint
    # Trigger processingTime='2 seconds' mengevaluasi aliran data setiap 2 detik
    streaming_query = cleaned_stream.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .trigger(processingTime=config["trigger_time"]) \
        .option("checkpointLocation", config["checkpoint_dir"]) \
        .start()

    logger.info("==========================================================")
    logger.info("Pipeline aktif streaming 24/7. Menunggu data transaksi...")
    logger.info("Tekan Ctrl+C di terminal untuk menghentikan proses.")
    logger.info("==========================================================")

    # 9. Menjaga program tetap hidup terus menerus mendengarkan stream
    try:
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Menerima sinyal penghentian dari user. Menghentikan streaming query...")
        streaming_query.stop()
        logger.info("Streaming query berhasil dihentikan secara aman.")


# Jalankan main() jika skrip dipanggil langsung
if __name__ == "__main__":
    main()
