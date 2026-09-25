-- ============================================================================
-- SKRIP INISIALISASI DATABASE POSTGRESQL (01-init.sql)
-- ============================================================================
-- Skrip ini otomatis dieksekusi oleh container PostgreSQL saat pertama kali
-- database dinyalakan (melalui direktori /docker-entrypoint-initdb.d/).
--
-- Tujuan:
-- 1. Membuat tabel 'retail_transactions' sebagai target persistensi data PySpark.
-- 2. Membuat tabel cadangan 'retail' untuk kompatibilitas skrip lama.
-- 3. Membangun indeks (indexing) pada kolom-kolom kunci untuk mempercepat query.
-- 4. Membuat Analytical View 'v_retail_live_summary' untuk dashboard live omset.
-- ============================================================================

-- Prosedur PL/pgSQL terpusat agar skema dapat diterapkan ke beberapa database
CREATE OR REPLACE PROCEDURE init_streaming_schema()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ------------------------------------------------------------------------
    -- 1. PEMBUATAN TABEL UTAMA: retail_transactions
    -- ------------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS retail_transactions (
        -- id: Primary key auto-increment 64-bit untuk menjamin keunikan setiap baris transaksi
        id BIGSERIAL PRIMARY KEY,
        
        -- invoice: Nomor nota belanja transaksi (panjang maksimal 50 karakter)
        invoice VARCHAR(50),
        
        -- stock_code: Kode unik identifikasi produk / SKU barang
        stock_code VARCHAR(50),
        
        -- description: Nama atau deskripsi produk (tipe TEXT fleksibel untuk teks panjang)
        description TEXT,
        
        -- quantity: Jumlah unit barang yang dibeli (bisa bernilai negatif jika retur)
        quantity INTEGER,
        
        -- invoice_date: Tanggal transaksi nota belanja dari dataset sumber
        invoice_date TIMESTAMP,
        
        -- price: Harga per unit barang dalam format desimal presisi ganda
        price DOUBLE PRECISION,
        
        -- customer_id: ID pembeli (menggunakan VARCHAR karena ada akun dengan kode alfanumerik)
        customer_id VARCHAR(50),
        
        -- country: Nama negara asal pembeli
        country VARCHAR(100),
        
        -- is_cancelled: Penanda boolean (TRUE jika nota dibatalkan/retur yang diawali huruf 'C')
        is_cancelled BOOLEAN DEFAULT FALSE,
        
        -- total_amount: Nilai omset belanja (|quantity| * price) yang dihitung oleh PySpark
        total_amount DOUBLE PRECISION,
        
        -- event_timestamp: Waktu aktual saat transaksi dipancarkan oleh Producer Simulator
        event_timestamp TIMESTAMP,
        
        -- processed_at: Waktu saat data berhasil diproses oleh PySpark dan dicatat ke PostgreSQL
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- ------------------------------------------------------------------------
    -- 2. PEMBUATAN INDEKS PERFORMA (PERFORMANCE INDEXING)
    -- ------------------------------------------------------------------------
    -- Mengapa butuh indeks?
    -- Saat tabel sudah berisi ratusan ribu baris, indeks B-Tree ini mencegah
    -- 'Full Table Scan' sehingga pencarian query menjadi sangat cepat.
    
    -- Mempercepat pencarian berdasarkan nomor faktur
    CREATE INDEX IF NOT EXISTS idx_retail_tx_invoice ON retail_transactions(invoice);
    -- Mempercepat analisis transaksi per pelanggan
    CREATE INDEX IF NOT EXISTS idx_retail_tx_customer ON retail_transactions(customer_id);
    -- Mempercepat agregasi atau filtering berdasarkan negara
    CREATE INDEX IF NOT EXISTS idx_retail_tx_country ON retail_transactions(country);
    -- Mempercepat pengurutan dan filtering berdasarkan waktu kirim event
    CREATE INDEX IF NOT EXISTS idx_retail_tx_event_ts ON retail_transactions(event_timestamp);
    -- Mempercepat kueri monitoring latensi berdasarkan waktu simpan
    CREATE INDEX IF NOT EXISTS idx_retail_tx_processed ON retail_transactions(processed_at);

    -- ------------------------------------------------------------------------
    -- 3. TABEL CADANGAN: retail (UNTUK KOMPATIBILITAS SKRIP LEGACY)
    -- ------------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS retail (
        id BIGSERIAL PRIMARY KEY,
        invoice VARCHAR(50),
        stock_code VARCHAR(50),
        description TEXT,
        quantity INTEGER,
        invoice_date TIMESTAMP,
        price DOUBLE PRECISION,
        customer_id VARCHAR(50),
        country VARCHAR(100),
        is_cancelled BOOLEAN DEFAULT FALSE,
        total_amount DOUBLE PRECISION,
        event_timestamp TIMESTAMP,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- ------------------------------------------------------------------------
    -- 4. ANALYTICAL VIEW: v_retail_live_summary
    -- ------------------------------------------------------------------------
    -- View ini bertindak sebagai 'Live Dashboard' virtual yang otomatis
    -- menghitung rangkuman performa penjualan per negara setiap kali di-query.
    CREATE OR REPLACE VIEW v_retail_live_summary AS
    SELECT 
        country,
        -- Menghitung total transaksi
        COUNT(*) AS total_transactions,
        -- Menghitung jumlah pesanan yang dibatalkan
        SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS total_cancelled,
        -- Menghitung total omset pendapatan kotor (pembulatan 2 desimal)
        ROUND(COALESCE(SUM(total_amount), 0)::numeric, 2) AS total_revenue,
        -- Menghitung rata-rata nilai belanja per pesanan
        ROUND(COALESCE(AVG(total_amount), 0)::numeric, 2) AS avg_order_value,
        -- Waktu event transaksi paling anyar
        MAX(event_timestamp) AS latest_event_time,
        -- Waktu data terakhir yang masuk ke database
        MAX(processed_at) AS last_processed_at
    FROM retail_transactions
    GROUP BY country
    ORDER BY total_revenue DESC;
END;
$$;

-- ----------------------------------------------------------------------------
-- EKSEKUSI PROSEDUR DI DATABASE TARGET & FALLBACK DATABASE
-- ----------------------------------------------------------------------------
-- Terapkan skema ke database utama 'retail_db'
\c retail_db;
CALL init_streaming_schema();

-- Terapkan juga ke database default 'postgres' sebagai fallback
-- jika siswa lupa memilih database 'retail_db' saat menghubungkan pgAdmin
\c postgres;
CALL init_streaming_schema();
