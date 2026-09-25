#!/usr/bin/env python3
"""
================================================================================
EVENT PRODUCER SIMULATOR (producer_simulator.py)
================================================================================
Modul ini bertindak sebagai simulator "Mesin Kasir / E-Commerce Store" yang
mengalirkan data transaksi penjualan secara real-time baris-demi-baris ke
Apache Kafka.

Alur Kerja:
1. Membaca dataset CSV (OnlineRetail.csv).
2. Melakukan normalisasi nama kolom dan struktur data.
3. Membubuhkan stempel waktu saat ini (event_timestamp / current_timestamp).
4. Mengubah data menjadi payload JSON.
5. Mengirimkannya ke Kafka topic dengan interval jeda acak (0.2 - 1.0 detik)
   untuk meniru perilaku transaksi manusia di dunia nyata.
================================================================================
"""

# ==============================================================================
# 1. IMPORT MODUL & PUSTAKA STANDAR
# ==============================================================================
import os                  # Berinteraksi dengan sistem operasi dan environment variables
import sys                 # Akses sistem interpreter Python (misal untuk sys.exit)
import csv                 # Membaca berkas format Comma-Separated Values (CSV)
import json                # Memanipulasi dan menyusun format JavaScript Object Notation (JSON)
import time                # Mengatur jeda pengiriman data (delay / sleep)
import random              # Menghasilkan angka acak untuk mensimulasikan latensi natural transaksi
import argparse            # Menangani parameter argumen baris perintah (CLI arguments)
import logging             # Menampilkan catatan aktivitas dan status aplikasi secara rapi
from datetime import datetime, timezone  # Menghasilkan stempel waktu (timestamp) berbasis UTC
from configparser import ConfigParser    # Membaca konfigurasi dari file eksternal (config.ini)

# ==============================================================================
# 2. KONFIGURASI LOGGING (Pencatatan Aktivitas Terminal)
# ==============================================================================
# Mengatur format teks log agar memuat waktu, level (INFO/WARNING/ERROR), label PRODUCER, dan pesan.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PRODUCER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Inisialisasi logger dengan nama khusus "ProducerSimulator"
logger = logging.getLogger("ProducerSimulator")


# ==============================================================================
# 3. FUNGSI PEMBACA ENVIRONMENT VARIABLE (.env)
# ==============================================================================
def load_env_file(env_path: str = ".env"):
    """
    Membaca berkas .env (jika ada) dan memasukkan variabelnya ke dalam sistem (os.environ).
    Ini memungkinkan pengaturan port atau kredensial tanpa perlu mengubah kode program.
    """
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Lewati baris kosong atau baris komentar yang diawali tanda '#'
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    key, val = key.strip(), val.strip().strip("'\"")
                    # Hanya tambahkan jika key belum disetel sebelumnya di lingkungan sistem
                    if key not in os.environ:
                        os.environ[key] = val


# ==============================================================================
# 4. FUNGSI PEMUAT KONFIGURASI PIPELINE
# ==============================================================================
def load_configurations(config_path: str = "config.ini"):
    """
    Memuat konfigurasi Kafka dan jalur dataset dari file config.ini,
    dengan prioritas fallback ke environment variable (.env) jika tersedia.
    
    Variabel Kunci:
    - topic: Nama antrean / saluran topik di Kafka (default: 'retail_stream').
    - bootstrap_servers: Alamat host dan port broker Kafka (default: 'localhost:9092').
    - csv_filepath: Lokasi file data CSV retail (default: './data/OnlineRetail.csv').
    """
    load_env_file(".env")
    config = ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)

    # 1. Menentukan nama topik Kafka
    topic = os.getenv(
        "KAFKA_TOPIC",
        config.get("kafka", "topic", fallback="retail_stream")
    )
    
    # 2. Menentukan alamat broker Kafka (misal kafka:29092 di Docker atau localhost:9092 di lokal)
    kafka_port = os.getenv("KAFKA_PORT", "9092")
    default_servers = f"localhost:{kafka_port}"
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        config.get("kafka", "bootstrap_servers", fallback=default_servers)
    )
    
    # 3. Menentukan lokasi file CSV
    csv_filepath = os.getenv(
        "CSV_FILEPATH",
        config.get("kafka", "csv_filepath", fallback="./data/OnlineRetail.csv")
    )

    return {
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "csv_filepath": csv_filepath,
    }


# ==============================================================================
# 5. FUNGSI VALIDATOR KEBERADAAN DATASET
# ==============================================================================
def ensure_dataset_exists(filepath: str):
    """
    Memastikan dataset tersedia di komputer siswa.
    Jika file tidak ditemukan, fungsi ini secara cerdas akan membuat file sampel
    berisi transaksi contoh sehingga pipeline tetap dapat berjalan tanpa error file missing.
    """
    # Jika path default ditemukan, langsung gunakan
    if os.path.exists(filepath):
        return filepath

    # Cek lokasi alternatif yang sering dipakai siswa
    alternatives = ["./data/data.csv", "./OnlineRetail.csv", "data/OnlineRetail.csv"]
    for alt in alternatives:
        if os.path.exists(alt):
            logger.info(f"Ditemukan dataset pada jalur alternatif: {alt}")
            return alt

    # Jika sama sekali tidak ada, buat folder dan file contoh otomatis
    logger.warning(f"Dataset tidak ditemukan di '{filepath}'. Membuat sampel dataset ritel otomatis...")
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)

    # Baris data simulasi awal (memuat transaksi normal dan transaksi retur berawalan 'C')
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

    logger.info(f"Sampel dataset berhasil dibuat pada: {filepath}")
    return filepath


# ==============================================================================
# 6. FUNGSI INISIALISASI KAFKA PRODUCER DENGAN RETRY LOGIC
# ==============================================================================
def create_kafka_producer(bootstrap_servers: str, max_retries: int = 10, retry_delay: int = 3):
    """
    Membangun koneksi ke broker Apache Kafka dengan mekanisme percobaan ulang (retry).
    
    Penjelasan Parameter & Logika Syntax:
    - bootstrap_servers: Daftar broker awal yang dipisahkan koma.
    - value_serializer: Mengubah data dictionary Python menjadi biner UTF-8.
      Kafka tidak mengenal tipe data Python; Kafka hanya menerima byte biner!
      Format: lambda v: json.dumps(v).encode("utf-8")
    - acks="all": Memastikan broker telah menulis data ke partisi sebelum mengirim konfirmasi balik (guaranteed delivery).
    - retries=3: Jumlah percobaan kirim ulang internal oleh driver Kafka jika terjadi kendala jaringan sementara.
    - max_retries: Menghindari error saat container Kafka masih dalam proses booting saat startup.
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error("Pustaka kafka-python belum terpasang! Jalankan: pip install -r requirements.txt")
        sys.exit(1)

    producer = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Menghubungkan ke Kafka broker di '{bootstrap_servers}' (Percobaan {attempt}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # Serialisasi objek ke string JSON lalu ke UTF-8 Bytes
                acks="all",                                                # Tingkat garansi pesan tertinggi
                retries=3,                                                 # Toleransi gangguan jaringan sementara
                request_timeout_ms=15000,                                  # Batas tunggu respon broker (15 detik)
                api_version=(0, 10, 1),                                    # Versi protokol kompatibilitas Kafka
            )
            logger.info("Berhasil terhubung ke Apache Kafka broker!")
            return producer
        except Exception as e:
            if attempt < max_retries:
                logger.warning(f"Broker Kafka belum siap ({e}). Mencoba lagi dalam {retry_delay} detik...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Gagal terhubung ke Kafka setelah {max_retries} kali percobaan: {e}")
                raise
    return producer


# ==============================================================================
# 7. FUNGSI NORMALISASI DATA TRANSAKSI
# ==============================================================================
def normalize_record(row: dict) -> dict:
    """
    Menyeragamkan kunci nama kolom dari baris CSV mentah menjadi standar yang konsisten,
    serta menyisipkan stempel waktu aktual (current_timestamp).
    
    Penjelasan Logika Syntax:
    - row.get("Invoice") or row.get("InvoiceNo") or ...:
      Teknik fallback defensif untuk mengantisipasi perbedaan penamaan header CSV pada berbagai versi dataset.
    - current_ts = datetime.now(timezone.utc).isoformat():
      Mencatat waktu persis dalam zona UTC ketika event transaksi ini dipancarkan oleh simulator.
    """
    # Mengambil nomor faktur (Invoice / InvoiceNo)
    invoice = row.get("Invoice") or row.get("InvoiceNo") or row.get("invoice") or ""
    # Mengambil kode barang / StockCode
    stock_code = row.get("StockCode") or row.get("stock_code") or ""
    # Mengambil deskripsi barang
    description = row.get("Description") or row.get("description") or ""
    # Mengambil jumlah unit belanja
    quantity = row.get("Quantity") or row.get("quantity") or "0"
    # Mengambil tanggal faktur dari dataset
    invoice_date = row.get("InvoiceDate") or row.get("invoice_date") or ""
    # Mengambil harga satuan barang
    price = row.get("Price") or row.get("UnitPrice") or row.get("price") or "0.0"
    # Mengambil ID pelanggan
    customer_id = row.get("CustomerID") or row.get("customer_id") or ""
    # Mengambil negara pembeli
    country = row.get("Country") or row.get("country") or "Unknown"

    # Waktu sekarang (UTC) saat event dipancarkan oleh kasir simulator
    current_ts = datetime.now(timezone.utc).isoformat()

    # Mengembalikan payload dictionary yang siap diserialisasi ke JSON
    return {
        "Invoice": str(invoice).strip(),
        "StockCode": str(stock_code).strip(),
        "Description": str(description).strip(),
        "Quantity": quantity,
        "InvoiceDate": str(invoice_date).strip(),
        "Price": price,
        "CustomerID": str(customer_id).strip(),
        "Country": str(country).strip(),
        "current_timestamp": current_ts,   # Waktu emit asli event
    }


# ==============================================================================
# 8. FUNGSI STREAMING DATA UTAMA (LOOP PENGIRIMAN DATA)
# ==============================================================================
def stream_data(
    producer,
    topic: str,
    filepath: str,
    continuous_loop: bool = True,
    min_delay: float = 0.2,
    max_delay: float = 1.0,
    max_messages: int = 0,
):
    """
    Membaca CSV baris-demi-baris dan memancarkan payload JSON ke topik Kafka
    dengan jeda acak natural (0.2s - 1.0s).
    
    Penjelasan Parameter:
    - producer: Instance KafkaProducer yang aktif.
    - topic: Nama topik tujuan di Kafka.
    - filepath: Lokasi file data CSV sumber.
    - continuous_loop: Jika True, data akan terus diulang (loop) saat mencapai akhir file agar simulasi 24/7 tetap berjalan.
    - min_delay, max_delay: Rentang waktu jeda antar event dalam detik.
    - max_messages: Batas maksimum pesan yang dikirim (0 artinya tanpa batas).
    """
    total_sent = 0  # Akumulator jumlah total transaksi yang berhasil dikirim
    cycle = 1       # Menghitung siklus perulangan pembacaan dataset

    logger.info(f"Memulai streaming real-time ke topik '{topic}' dari file '{filepath}'")
    logger.info(f"Rentang jeda: {min_delay}s - {max_delay}s | Mode perulangan 24/7: {continuous_loop}")

    try:
        while True:
            logger.info(f"--- Memulai Siklus Data #{cycle} ---")
            with open(filepath, "r", encoding="utf-8", errors="replace") as csv_file:
                # DictReader memetakan setiap baris CSV langsung menjadi kamus Python {kolom: nilai}
                reader = csv.DictReader(csv_file)
                for row_idx, raw_row in enumerate(reader, start=1):
                    # 1. Normalisasi dan beri stempel waktu aktual
                    payload = normalize_record(raw_row)

                    # 2. Kirim pesan ke Kafka (asinkron)
                    future = producer.send(topic, value=payload)
                    total_sent += 1

                    # 3. Tampilkan informasi event di konsol terminal
                    logger.info(
                        f"Terkirim #{total_sent:05d} | Topik: {topic} | "
                        f"Invoice: {payload['Invoice']} | "
                        f"Barang: {payload['Description'][:25]}... | "
                        f"Qty: {payload['Quantity']} | Harga: ${payload['Price']} | "
                        f"Negara: {payload['Country']} | "
                        f"Waktu: {payload['current_timestamp']}"
                    )

                    # Cek apakah batas kuota pesan tercapai
                    if max_messages > 0 and total_sent >= max_messages:
                        logger.info(f"Mencapai batas yang ditentukan ({max_messages} pesan). Menghentikan streaming.")
                        return

                    # 4. Simulasi jeda waktu natural antar pembeli (0.2 hingga 1.0 detik)
                    delay = random.uniform(min_delay, max_delay)
                    time.sleep(delay)

            # Jika mode loop dimatikan oleh siswa lewat parameter --no-loop
            if not continuous_loop:
                logger.info("Mencapai baris terakhir dataset dan mode loop dinonaktifkan. Pengiriman selesai.")
                break

            cycle += 1
            logger.info("Satu iterasi dataset selesai dibaca. Mengulang kembali untuk streaming kontinu 24/7...")
            time.sleep(1.0)

    except KeyboardInterrupt:
        logger.info("\nStreaming dihentikan secara manual oleh pengguna (Ctrl+C). Mengosongkan buffer producer...")
    finally:
        # PENTING: Mengosongkan memori buffer dan menutup koneksi socket secara bersih
        producer.flush()
        producer.close()
        logger.info(f"Koneksi producer ditutup dengan aman. Total transaksi yang dialirkan: {total_sent}")


# ==============================================================================
# 9. TITIK MASUK UTAMA PROGRAM (ENTRY POINT & CLI PARSER)
# ==============================================================================
def main():
    """
    Titik masuk utama skrip. Mendukung argumen baris perintah kustom untuk fleksibilitas praktikum siswa.
    Contoh: python3 producer_simulator.py --min-delay 0.1 --max-delay 0.5
    """
    parser = argparse.ArgumentParser(description="Streaming Producer Simulator untuk Apache Kafka")
    parser.add_argument("--config", default="config.ini", help="Jalur ke file konfigurasi config.ini")
    parser.add_argument("--topic", default=None, help="Nama topik Kafka tujuan")
    parser.add_argument("--bootstrap-servers", default=None, help="Alamat broker Kafka (host:port)")
    parser.add_argument("--file", default=None, help="Jalur ke berkas dataset CSV")
    parser.add_argument("--min-delay", type=float, default=0.2, help="Jeda minimum antar pesan dalam detik")
    parser.add_argument("--max-delay", type=float, default=1.0, help="Jeda maksimum antar pesan dalam detik")
    parser.add_argument("--max-messages", type=int, default=0, help="Batasi jumlah pesan (0 untuk tanpa batas)")
    parser.add_argument("--no-loop", action="store_true", help="Jangan mengulang dataset setelah baris terakhir")

    args = parser.parse_args()

    # 1. Pemuatan konfigurasi sistem
    cfg = load_configurations(args.config)
    topic = args.topic or cfg["topic"]
    bootstrap_servers = args.bootstrap_servers or cfg["bootstrap_servers"]
    csv_filepath = args.file or cfg["csv_filepath"]

    # 2. Verifikasi ketersediaan dataset
    resolved_filepath = ensure_dataset_exists(csv_filepath)

    # 3. Buat koneksi ke broker Kafka
    producer = create_kafka_producer(bootstrap_servers)

    # 4. Jalankan pengaliran data streaming
    stream_data(
        producer=producer,
        topic=topic,
        filepath=resolved_filepath,
        continuous_loop=not args.no_loop,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_messages=args.max_messages,
    )


# Menjalankan fungsi main() hanya jika skrip dieksekusi secara langsung (bukan saat di-import modul lain)
if __name__ == "__main__":
    main()
