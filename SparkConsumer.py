#!/usr/bin/env python3
"""
================================================================================
SPARK CONSUMER ENTRY WRAPPER (SparkConsumer.py)
================================================================================
Berkas pembungkus (wrapper) ini dipertahankan untuk kompatibilitas mundur
(backwards compatibility) dengan tugas/repositori awal.

File ini secara otomatis meneruskan eksekusi ke modul consumer utama:
`spark_streaming.py`.
================================================================================
"""

# Mengimpor fungsi main dari spark_streaming
from spark_streaming import main

if __name__ == "__main__":
    # Menjalankan fungsi utama PySpark Structured Streaming consumer
    main()