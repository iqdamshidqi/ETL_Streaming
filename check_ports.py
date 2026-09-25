#!/usr/bin/env python3
"""
Port Conflict Checker & Environment Validator (check_ports.py)
--------------------------------------------------------------
Mengecek apakah port default pipeline (5432, 5050, 9092, 2181)
sedang digunakan oleh aplikasi lain di host (sangat berguna untuk
mendeteksi port bentrok di laptop Windows / Mac mahasiswa).
"""

import socket
import os
import sys
from configparser import ConfigParser

# Warna terminal ANSI (cross-platform)
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"


def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """Mengecek apakah suatu port sedang aktif / digunakan di localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        result = s.connect_ex((host, port))
        return result == 0  # 0 artinya port terbuka dan menerima koneksi (sedang terpakai)


def load_env_or_default():
    """Membaca port dari .env atau default."""
    ports = {
        "POSTGRES_PORT": ("PostgreSQL Database", 5432, 5433),
        "PGADMIN_PORT": ("pgAdmin Web UI", 5050, 5051),
        "KAFKA_PORT": ("Kafka Broker", 9092, 9093),
        "ZOOKEEPER_PORT": ("Zookeeper", 2181, 2182),
    }

    env_values = {}
    if os.path.exists(".env"):
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    env_values[key.strip()] = val.strip()

    resolved = {}
    for var_name, (service_label, default_port, alt_port) in ports.items():
        actual_port = int(env_values.get(var_name, os.getenv(var_name, default_port)))
        resolved[var_name] = {
            "label": service_label,
            "port": actual_port,
            "alt": alt_port,
        }
    return resolved


def main():
    print(f"\n{CYAN}======================================================================{RESET}")
    print(f"{CYAN}  PRE-FLIGHT PORT CHECKER - ETL STREAMING PIPELINE{RESET}")
    print(f"{CYAN}======================================================================{RESET}")
    print("Mengecek ketersediaan port di laptop Anda sebelum menjalankan Docker...\n")

    port_configs = load_env_or_default()
    has_conflict = False

    for var_name, info in port_configs.items():
        label = info["label"]
        port = info["port"]
        alt_port = info["alt"]

        in_use = is_port_in_use(port)
        if in_use:
            has_conflict = True
            print(f" {RED}[!] BENTROK{RESET}  : Port {YELLOW}{port}{RESET} ({label}) {RED}SEDANG DIGUNAKAN{RESET} oleh aplikasi lain!")
            print(f"               -> Solusi: Buka file {CYAN}.env{RESET} dan ubah:")
            print(f"                  {YELLOW}{var_name}={alt_port}{RESET}\n")
        else:
            print(f" {GREEN}[OK] AMAN{RESET}    : Port {GREEN}{port}{RESET} ({label}) siap digunakan.")

    print(f"\n{CYAN}----------------------------------------------------------------------{RESET}")
    if has_conflict:
        print(f"{RED}PERINGATAN:{RESET} Terdeteksi port yang sudah terpakai di sistem ini.")
        print(f"Jika Anda menjalankan 'docker compose up -d' sekarang, Docker akan error 'port already in use'.")
        print(f"Silakan sesuaikan nilai port yang bentrok di file {CYAN}.env{RESET}, lalu jalankan script ini lagi.")
        sys.exit(1)
    else:
        print(f"{GREEN}SEMUA PORT BERSIH!{RESET} Tidak ada konflik. Anda aman menjalankan:")
        print(f"  {CYAN}docker compose up -d{RESET}\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
