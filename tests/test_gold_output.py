from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent

GOLD_DB = PROJECT_ROOT / "database" / "data_center_gold.duckdb"

SILVER_GLOB = str(
    PROJECT_ROOT / "data" / "silver" / "telemetry" / "**" / "*.parquet"
)


def main():
    connection = duckdb.connect(":memory:")

    gold_path = str(GOLD_DB).replace("'", "''")

    connection.execute(
        f"ATTACH '{gold_path}' AS gold (READ_ONLY)"
    )

    try:
        silver_path = SILVER_GLOB.replace("'", "''")

        connection.execute(f"""
            CREATE VIEW silver_telemetry AS
            SELECT *
            FROM read_parquet('{silver_path}')
        """)

        print("\n========== GOLD QUALITY REPORT ==========")

        # 1. Dimension counts
        dimensions = connection.execute("""
            SELECT
                (SELECT COUNT(*) FROM gold.dim_datacenter),
                (SELECT COUNT(*) FROM gold.dim_server),
                (SELECT COUNT(*) FROM gold.dim_date)
        """).fetchone()

        print("\nDimension counts:")
        print(f"Data centers: {dimensions[0]}")
        print(f"Servers: {dimensions[1]}")
        print(f"Dates: {dimensions[2]}")

        assert dimensions == (5, 500, 1)

        # 2. Fact counts
        facts = connection.execute("""
            SELECT
                (SELECT COUNT(*) FROM gold.fact_telemetry),
                (SELECT COUNT(*) FROM gold.fact_alerts)
        """).fetchone()

        print("\nFact counts:")
        print(f"Telemetry: {facts[0]}")
        print(f"Alerts: {facts[1]}")

        assert facts[0] == 500

        expected_alerts = connection.execute("""
            SELECT COUNT(*)
            FROM silver_telemetry
            WHERE is_anomaly = TRUE
        """).fetchone()[0]

        assert facts[1] == expected_alerts

        # 3. Check primary key uniqueness
        duplicate_keys = connection.execute("""
            SELECT
                (SELECT COUNT(*) FROM (
                    SELECT telemetry_key
                    FROM gold.fact_telemetry
                    GROUP BY telemetry_key
                    HAVING COUNT(*) > 1
                )) +
                (SELECT COUNT(*) FROM (
                    SELECT alert_key
                    FROM gold.fact_alerts
                    GROUP BY alert_key
                    HAVING COUNT(*) > 1
                ))
        """).fetchone()[0]

        print(f"\nDuplicate fact keys: {duplicate_keys}")
        assert duplicate_keys == 0

        # 4. Verify foreign key relationships
        orphan_count = connection.execute("""
            SELECT COUNT(*)
            FROM gold.fact_telemetry f
            LEFT JOIN gold.dim_server s
                ON f.server_key = s.server_key
            LEFT JOIN gold.dim_datacenter d
                ON f.datacenter_key = d.datacenter_key
            LEFT JOIN gold.dim_date dt
                ON f.date_key = dt.date_key
            WHERE
                s.server_key IS NULL
                OR d.datacenter_key IS NULL
                OR dt.date_key IS NULL
        """).fetchone()[0]

        print(f"Orphan telemetry records: {orphan_count}")
        assert orphan_count == 0

        # 5. Compare Silver and Gold telemetry measurements
        differences = connection.execute("""
            SELECT COUNT(*)
            FROM silver_telemetry s
            JOIN gold.dim_server ds
                ON s.server_id = ds.server_id
            JOIN gold.fact_telemetry f
                ON f.server_key = ds.server_key
               AND f.timestamp = s.timestamp
            WHERE
                s.cpu_utilization IS DISTINCT FROM f.cpu_utilization
                OR s.memory_utilization IS DISTINCT FROM f.memory_utilization
                OR s.disk_utilization IS DISTINCT FROM f.disk_utilization
                OR s.temperature IS DISTINCT FROM f.temperature
                OR s.power_consumption IS DISTINCT FROM f.power_consumption
                OR s.is_anomaly IS DISTINCT FROM f.is_anomaly
        """).fetchone()[0]

        print(f"Silver-Gold metric differences: {differences}")
        assert differences == 0

        # 6. Verify every alert corresponds to an anomalous telemetry record
        unmatched_alerts = connection.execute("""
            SELECT COUNT(*)
            FROM gold.fact_alerts a
            LEFT JOIN gold.fact_telemetry t
                ON a.server_key = t.server_key
               AND a.timestamp = t.timestamp
            WHERE
                t.telemetry_key IS NULL
                OR t.is_anomaly = FALSE
        """).fetchone()[0]

        print(f"Unmatched alert records: {unmatched_alerts}")
        assert unmatched_alerts == 0

        # 7. Test an analytical star-schema join
        print("\nTelemetry by Data Center:")

        datacenter_report = connection.execute("""
            SELECT
                d.datacenter_name,
                COUNT(*) AS telemetry_records,
                ROUND(AVG(f.cpu_utilization), 2) AS avg_cpu,
                ROUND(AVG(f.memory_utilization), 2) AS avg_memory,
                SUM(CASE WHEN f.is_anomaly THEN 1 ELSE 0 END)
                    AS anomaly_count
            FROM gold.fact_telemetry f
            JOIN gold.dim_datacenter d
                ON f.datacenter_key = d.datacenter_key
            GROUP BY d.datacenter_name
            ORDER BY d.datacenter_name
        """).df()

        print(datacenter_report.to_string(index=False))

        assert len(datacenter_report) == 5
        assert datacenter_report["telemetry_records"].sum() == 500
        assert datacenter_report["anomaly_count"].sum() == expected_alerts

        print("\nGold quality checks passed!")

    finally:
        connection.close()


if __name__ == "__main__":
    main()