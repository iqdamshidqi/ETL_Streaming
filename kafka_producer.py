#!/usr/bin/env python3
"""
================================================================================
KAFKA PRODUCER ENTRY WRAPPER (kafka_producer.py)
================================================================================
Berkas pembungkus (wrapper) ini dipertahankan untuk kompatibilitas mundur
(backwards compatibility) dengan tugas/repositori awal.

File ini secara otomatis meneruskan eksekusi ke modul simulator utama:
`producer_simulator.py`.
================================================================================
"""

# Mengimpor fungsi main dari producer_simulator
from producer_simulator import main

if __name__ == "__main__":
    # Menjalankan fungsi utama simulator kasir streaming
    main()