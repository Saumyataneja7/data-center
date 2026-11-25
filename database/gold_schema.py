from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASE_DIR = PROJECT_ROOT / "database"
GOLD_DB = DATABASE_DIR / "data_center_gold.duckdb"


def create_gold_schema():

    DATABASE_DIR.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(GOLD_DB))

    # Dimension: Data Center
    connection.execute("""
        CREATE TABLE IF NOT EXISTS dim_datacenter (
            datacenter_key INTEGER PRIMARY KEY,
            datacenter_id VARCHAR UNIQUE NOT NULL,
            datacenter_name VARCHAR NOT NULL,
            city VARCHAR NOT NULL,
            country VARCHAR NOT NULL
        )
    """)

    # Dimension: Server
    connection.execute("""
        CREATE TABLE IF NOT EXISTS dim_server (
            server_key INTEGER PRIMARY KEY,
            server_id VARCHAR UNIQUE NOT NULL,
            datacenter_key INTEGER NOT NULL,
            rack_id VARCHAR,
            server_type VARCHAR,
            operating_system VARCHAR,
            cpu_cores INTEGER,
            memory_gb INTEGER
        )
    """)

    # Dimension: Date
    connection.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE UNIQUE NOT NULL,
            day INTEGER,
            month INTEGER,
            month_name VARCHAR,
            quarter INTEGER,
            year INTEGER,
            day_of_week VARCHAR,
            is_weekend BOOLEAN
        )
    """)

    # Fact: Telemetry
    connection.execute("""
        CREATE TABLE IF NOT EXISTS fact_telemetry (
            telemetry_key BIGINT PRIMARY KEY,
            server_key INTEGER NOT NULL,
            datacenter_key INTEGER NOT NULL,
            date_key INTEGER NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            telemetry_hour INTEGER,
            cpu_utilization DOUBLE,
            memory_utilization DOUBLE,
            disk_utilization DOUBLE,
            network_in_mbps DOUBLE,
            network_out_mbps DOUBLE,
            temperature DOUBLE,
            power_consumption DOUBLE,
            fan_speed_rpm DOUBLE,
            server_status VARCHAR,
            is_anomaly BOOLEAN
        )
    """)

    # Fact: Alerts
    connection.execute("""
        CREATE TABLE IF NOT EXISTS fact_alerts (
            alert_key BIGINT PRIMARY KEY,
            server_key INTEGER NOT NULL,
            datacenter_key INTEGER NOT NULL,
            date_key INTEGER NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            anomaly_type VARCHAR NOT NULL,
            anomaly_severity VARCHAR NOT NULL,
            cpu_utilization DOUBLE,
            memory_utilization DOUBLE,
            disk_utilization DOUBLE,
            temperature DOUBLE,
            power_consumption DOUBLE,
            server_status VARCHAR
        )
    """)

    connection.close()

    print("Gold schema created successfully.")
    print(f"Database: {GOLD_DB}")


if __name__ == "__main__":
    create_gold_schema()