-- ============================================================================
-- Initialization Script for Real-Time Retail Streaming Pipeline
-- Automatically executed by PostgreSQL container upon initial startup
-- ============================================================================

-- Function to initialize schema
CREATE OR REPLACE PROCEDURE init_streaming_schema()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS retail_transactions (
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

    CREATE INDEX IF NOT EXISTS idx_retail_tx_invoice ON retail_transactions(invoice);
    CREATE INDEX IF NOT EXISTS idx_retail_tx_customer ON retail_transactions(customer_id);
    CREATE INDEX IF NOT EXISTS idx_retail_tx_country ON retail_transactions(country);
    CREATE INDEX IF NOT EXISTS idx_retail_tx_event_ts ON retail_transactions(event_timestamp);
    CREATE INDEX IF NOT EXISTS idx_retail_tx_processed ON retail_transactions(processed_at);

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

    CREATE OR REPLACE VIEW v_retail_live_summary AS
    SELECT 
        country,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS total_cancelled,
        ROUND(COALESCE(SUM(total_amount), 0)::numeric, 2) AS total_revenue,
        ROUND(COALESCE(AVG(total_amount), 0)::numeric, 2) AS avg_order_value,
        MAX(event_timestamp) AS latest_event_time,
        MAX(processed_at) AS last_processed_at
    FROM retail_transactions
    GROUP BY country
    ORDER BY total_revenue DESC;
END;
$$;

-- Apply to retail_db
\c retail_db;
CALL init_streaming_schema();

-- Apply to postgres default db as fallback
\c postgres;
CALL init_streaming_schema();

