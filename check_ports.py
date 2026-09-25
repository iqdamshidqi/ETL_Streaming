#!/usr/bin/env python3
"""
================================================================================
PRE-FLIGHT PORT CONFLICT CHECKER (check_ports.py)
================================================================================
Alat bantu diagnostik untuk mendeteksi apakah port yang dibutuhkan oleh Docker
(PostgreSQL: 5432, pgAdmin: 5050, Kafka: 9092, Zookeeper: 2181) sedang bentrok
karena telah digunakan oleh aplikasi lain di komputer/laptop siswa.

Cara Kerja:
1. Membaca port yang akan dipakai dari file .env (jika ada) atau menggunakan port default.
2. Membuka socket TCP sementara ke setiap port di localhost (127.0.0.1).
3. Jika koneksi berhasil (return code 0), berarti port sedang aktif dipakai (BENTROK).
4. Jika koneksi ditolak/gagal, berarti port sedang kosong dan siap dipakai (AMAN).
5. Memberikan instruksi solusi instan ke siswa jika terjadi bentrok port.
================================================================================
"""

# ==============================================================================
# 1. IMPORT MODUL BAWAAN PYTHON
# ==============================================================================
import socket              # Pustaka jaringan standar untuk menguji koneksi TCP/IP
import os                  # Memeriksa keberadaan file .env di direktori lokal
import sys                 # Mengatur kode keluar terminal (sys.exit)
from configparser import ConfigParser

# ==============================================================================
# 2. DEFINISI KODE WARNA ANSI TERMINAL (Cross-Platform)
# ==============================================================================
# Memberikan output warna agar mudah dipahami siswa (Hijau = Aman, Merah = Bahaya)
GREEN = "\033[92m"         # Warna teks hijau untuk status aman
RED = "\033[91m"           # Warna teks merah untuk status bentrok
YELLOW = "\033[93m"        # Warna teks kuning untuk angka port dan peringatan
CYAN = "\033[96m"          # Warna teks cyan untuk judul dan instruksi
RESET = "\033[0m"          # Mengembalikan warna teks ke normal


# ==============================================================================
# 3. FUNGSI PENGECEKAN KETERSEDIAAN PORT DENGAN SOCKET TCP
# ==============================================================================
def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """
    Mencoba melakukan koneksi ke host dan port tertentu.
    
    Logika Syntax:
    - socket.socket(socket.AF_INET, socket.SOCK_STREAM):
      Membuat objek socket IPv4 (AF_INET) berbasis protokol TCP (SOCK_STREAM).
    - s.settimeout(0.5):
      Membatasi waktu tunggu respon maksimal 0.5 detik agar pengecekan berlangsung cepat.
    - s.connect_ex((host, port)):
      Mengembalikan angka 0 jika koneksi BERHASIL diterima oleh aplikasi yang sedang aktif di port tsb.
      Jika mengembalikan angka selain 0 (misal Connection Refused), berarti port kosong.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        result = s.connect_ex((host, port))
        return result == 0  # True jika port sedang digunakan


# ==============================================================================
# 4. FUNGSI MEMBACA PORT DARI .ENV ATAU MENGGUNAKAN DEFAULT
# ==============================================================================
def load_env_or_default():
    """
    Memetakan daftar port yang akan dicek.
    Format kamus:
    "NAMA_VAR": ("Nama Layanan", Port_Default, Port_Alternatif_Jika_Bentrok)
    """
    ports = {
        "POSTGRES_PORT": ("PostgreSQL Database", 5432, 5433),
        "PGADMIN_PORT": ("pgAdmin Web UI", 5050, 5051),
        "KAFKA_PORT": ("Kafka Broker", 9092, 9093),
        "ZOOKEEPER_PORT": ("Zookeeper", 2181, 2182),
    }

    env_values = {}
    # Jika file .env sudah dibuat oleh siswa, baca isinya
    if os.path.exists(".env"):
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    env_values[key.strip()] = val.strip()

    resolved = {}
    for var_name, (service_label, default_port, alt_port) in ports.items():
        # Ambil nilai dari .env, jika tidak ada gunakan default_port
        actual_port = int(env_values.get(var_name, os.getenv(var_name, default_port)))
        resolved[var_name] = {
            "label": service_label,
            "port": actual_port,
            "alt": alt_port,
        }
    return resolved


# ==============================================================================
# 5. TITIK MASUK UTAMA PROGRAM (MAIN FUNCTION)
# ==============================================================================
def main():
    print(f"\n{CYAN}======================================================================{RESET}")
    print(f"{CYAN}  PRE-FLIGHT PORT CHECKER - ETL STREAMING PIPELINE{RESET}")
    print(f"{CYAN}======================================================================{RESET}")
    print("Mengecek ketersediaan port di laptop Anda sebelum menjalankan Docker...\n")

    port_configs = load_env_or_default()
    has_conflict = False

    # Lakukan pengujian satu per satu ke seluruh port
    for var_name, info in port_configs.items():
        label = info["label"]
        port = info["port"]
        alt_port = info["alt"]

        in_use = is_port_in_use(port)
        if in_use:
            has_conflict = True
            print(f" {RED}[!] BENTROK{RESET}  : Port {YELLOW}{port}{RESET} ({label}) {RED}SEDANG DIGUNAKAN{RESET} oleh aplikasi lain!")
            print(f"               -> Solusi: Buka file {CYAN}.env{RESET} dan ubah nilainya menjadi:")
            print(f"                  {YELLOW}{var_name}={alt_port}{RESET}\n")
        else:
            print(f" {GREEN}[OK] AMAN{RESET}    : Port {GREEN}{port}{RESET} ({label}) siap digunakan.")

    print(f"\n{CYAN}----------------------------------------------------------------------{RESET}")
    # Berikan kesimpulan akhir kepada siswa
    if has_conflict:
        print(f"{RED}PERINGATAN:{RESET} Terdeteksi port yang sudah terpakai di sistem Anda.")
        print(f"Jika Anda menjalankan 'docker compose up -d' sekarang, Docker akan error 'port already in use'.")
        print(f"Silakan sesuaikan nilai port yang bentrok di file {CYAN}.env{RESET}, lalu jalankan script ini lagi.")
        sys.exit(1)  # Keluar dengan kode error 1
    else:
        print(f"{GREEN}SEMUA PORT BERSIH!{RESET} Tidak ada konflik. Anda aman untuk menjalankan:")
        print(f"  {CYAN}docker compose up -d{RESET}\n")
        sys.exit(0)  # Keluar dengan kode sukses 0


if __name__ == "__main__":
    main()
