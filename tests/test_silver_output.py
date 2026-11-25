from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_FILE = (
    PROJECT_ROOT
    / "data/raw/telemetry/date=2026-11-25/batch=1830.parquet"
)

SILVER_FILE = (
    PROJECT_ROOT
    / "data/silver/telemetry/date=2026-11-25/batch=1830.parquet"
)


def main():
    assert RAW_FILE.exists(), "Raw Parquet file not found."
    assert SILVER_FILE.exists(), "Silver Parquet file not found."

    connection = duckdb.connect(database=":memory:")

    raw_path = str(RAW_FILE).replace("'", "''")
    silver_path = str(SILVER_FILE).replace("'", "''")

    connection.execute(f"""
        CREATE VIEW raw_telemetry AS
        SELECT * FROM read_parquet('{raw_path}')
    """)

    connection.execute(f"""
        CREATE VIEW silver_telemetry AS
        SELECT * FROM read_parquet('{silver_path}')
    """)

    print("\n========== SILVER QUALITY REPORT ==========")

    # 1. Compare record counts.
    counts = connection.execute("""
        SELECT
            (SELECT COUNT(*) FROM raw_telemetry),
            (SELECT COUNT(*) FROM silver_telemetry)
    """).fetchone()

    raw_count, silver_count = counts

    print(f"\nRaw records: {raw_count}")
    print(f"Silver records: {silver_count}")

    assert raw_count == 500
    assert silver_count == raw_count

    # 2. Check duplicate server records.
    duplicates = connection.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT server_id, timestamp
            FROM silver_telemetry
            GROUP BY server_id, timestamp
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    print(f"Duplicate records: {duplicates}")
    assert duplicates == 0

    # 3. Confirm original metrics were preserved.
    differences = connection.execute("""
        SELECT COUNT(*)
        FROM raw_telemetry r
        JOIN silver_telemetry s
          ON r.server_id = s.server_id
         AND r.timestamp = s.timestamp
        WHERE
            r.cpu_utilization IS DISTINCT FROM s.cpu_utilization
            OR r.memory_utilization IS DISTINCT FROM s.memory_utilization
            OR r.disk_utilization IS DISTINCT FROM s.disk_utilization
            OR r.temperature IS DISTINCT FROM s.temperature
            OR r.power_consumption IS DISTINCT FROM s.power_consumption
            OR r.is_anomaly IS DISTINCT FROM s.is_anomaly
            OR r.anomaly_type IS DISTINCT FROM s.anomaly_type
    """).fetchone()[0]

    print(f"Original metric differences: {differences}")
    assert differences == 0

    # 4. Check derived columns.
    derived_columns = connection.execute("""
        SELECT
            COUNT(*) FILTER (
                WHERE cpu_utilization_band !=
                    CASE
                        WHEN cpu_utilization < 40 THEN 'Low'
                        WHEN cpu_utilization < 70 THEN 'Moderate'
                        WHEN cpu_utilization < 85 THEN 'High'
                        ELSE 'Critical'
                    END
            ) AS invalid_cpu_bands,

            COUNT(*) FILTER (
                WHERE memory_utilization_band !=
                    CASE
                        WHEN memory_utilization < 50 THEN 'Low'
                        WHEN memory_utilization < 75 THEN 'Moderate'
                        WHEN memory_utilization < 90 THEN 'High'
                        ELSE 'Critical'
                    END
            ) AS invalid_memory_bands,

            COUNT(*) FILTER (
                WHERE temperature_band !=
                    CASE
                        WHEN temperature < 30 THEN 'Normal'
                        WHEN temperature < 45 THEN 'Elevated'
                        WHEN temperature < 60 THEN 'High'
                        ELSE 'Critical'
                    END
            ) AS invalid_temperature_bands

        FROM silver_telemetry
    """).fetchone()

    print(f"Invalid CPU bands: {derived_columns[0]}")
    print(f"Invalid memory bands: {derived_columns[1]}")
    print(f"Invalid temperature bands: {derived_columns[2]}")

    assert all(value == 0 for value in derived_columns)

    # 5. Check date and hour extraction.
    date_hour_errors = connection.execute("""
        SELECT COUNT(*)
        FROM silver_telemetry
        WHERE
            telemetry_date != CAST(timezone('Asia/Kolkata', timestamp) AS DATE)
            OR telemetry_hour != EXTRACT(HOUR FROM timezone('Asia/Kolkata', timestamp))
    """).fetchone()[0]

    print(f"Date/hour derivation errors: {date_hour_errors}")
    assert date_hour_errors == 0

    # 6. Display utilization distribution.
    print("\nCPU Utilization Band Distribution:")

    cpu_distribution = connection.execute("""
        SELECT
            cpu_utilization_band,
            COUNT(*) AS server_count
        FROM silver_telemetry
        GROUP BY cpu_utilization_band
        ORDER BY server_count DESC
    """).df()

    print(cpu_distribution.to_string(index=False))

    # 7. Display temperature distribution.
    print("\nTemperature Band Distribution:")

    temperature_distribution = connection.execute("""
        SELECT
            temperature_band,
            COUNT(*) AS server_count
        FROM silver_telemetry
        GROUP BY temperature_band
        ORDER BY server_count DESC
    """).df()

    print(temperature_distribution.to_string(index=False))

    print("\nSilver quality checks passed!")

    connection.close()


if __name__ == "__main__":
    main()