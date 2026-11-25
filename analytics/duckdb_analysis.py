from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "telemetry"

PARQUET_GLOB = str(RAW_DATA_DIR / "**" / "*.parquet")


def main():
    # Create an in-memory DuckDB connection.
    connection = duckdb.connect(database=":memory:")

    # Read all available Parquet batches directly.
    telemetry = connection.from_parquet(PARQUET_GLOB)

    # Register the Parquet data as a SQL view.
    telemetry.create_view("telemetry")

    print("\n========== DATA CENTER ANALYTICS ==========")

    # 1. Overall dataset summary
    print("\n1. Overall Telemetry Summary")

    summary = connection.execute("""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT server_id) AS unique_servers,
            COUNT(DISTINCT datacenter_id) AS datacenters,
            ROUND(AVG(cpu_utilization), 2) AS avg_cpu,
            ROUND(AVG(memory_utilization), 2) AS avg_memory,
            ROUND(AVG(temperature), 2) AS avg_temperature
        FROM telemetry
    """).df()

    print(summary.to_string(index=False))

    # 2. Records by data center
    print("\n2. Records by Data Center")

    datacenter_summary = connection.execute("""
        SELECT
            datacenter_name,
            city,
            COUNT(*) AS total_records,
            ROUND(AVG(cpu_utilization), 2) AS avg_cpu,
            ROUND(AVG(memory_utilization), 2) AS avg_memory
        FROM telemetry
        GROUP BY datacenter_name, city
        ORDER BY datacenter_name
    """).df()

    print(datacenter_summary.to_string(index=False))

    # 3. Server health distribution
    print("\n3. Server Health Distribution")

    health_summary = connection.execute("""
        SELECT
            server_status,
            COUNT(*) AS server_count
        FROM telemetry
        GROUP BY server_status
        ORDER BY server_count DESC
    """).df()

    print(health_summary.to_string(index=False))

    # 4. Anomaly distribution
    print("\n4. Anomaly Distribution")

    anomaly_summary = connection.execute("""
        SELECT
            anomaly_type,
            COUNT(*) AS anomaly_count
        FROM telemetry
        WHERE is_anomaly = TRUE
        GROUP BY anomaly_type
        ORDER BY anomaly_count DESC
    """).df()

    print(anomaly_summary.to_string(index=False))

    # 5. Average CPU by server type
    print("\n5. CPU Utilization by Server Type")

    server_type_summary = connection.execute("""
        SELECT
            server_type,
            COUNT(*) AS total_records,
            ROUND(AVG(cpu_utilization), 2) AS avg_cpu,
            ROUND(MAX(cpu_utilization), 2) AS max_cpu
        FROM telemetry
        GROUP BY server_type
        ORDER BY avg_cpu DESC
    """).df()

    print(server_type_summary.to_string(index=False))

    # 6. High CPU servers
    print("\n6. Servers with CPU Utilization Above 80%")

    high_cpu = connection.execute("""
        SELECT
            server_id,
            datacenter_name,
            server_type,
            cpu_utilization,
            server_status,
            is_anomaly
        FROM telemetry
        WHERE cpu_utilization > 80
        ORDER BY cpu_utilization DESC
        LIMIT 10
    """).df()

    if high_cpu.empty:
        print("No servers exceeded 80% CPU utilization in this batch.")
    else:
        print(high_cpu.to_string(index=False))

    connection.close()


if __name__ == "__main__":
    main()