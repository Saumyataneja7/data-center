from pathlib import Path

import duckdb

from utils.logger import get_logger

logger = get_logger("gold_facts", "gold_facts.log")


PROJECT_ROOT = Path(__file__).resolve().parent.parent

GOLD_DB = PROJECT_ROOT / "database" / "data_center_gold.duckdb"

SILVER_GLOB = str(
    PROJECT_ROOT / "data" / "silver" / "telemetry" / "**" / "*.parquet"
)


def populate_facts():

    connection = duckdb.connect(str(GOLD_DB))

    try:
        connection.execute("BEGIN TRANSACTION")

        connection.execute(f"""
            CREATE OR REPLACE VIEW silver_telemetry AS
            SELECT *
            FROM read_parquet('{SILVER_GLOB}')
        """)

        # Join Silver records with their dimension keys.
        connection.execute("""
            CREATE OR REPLACE TEMP VIEW enriched_telemetry AS
            SELECT
                s.*,
                ds.server_key,
                dc.datacenter_key,
                dd.date_key,

                CAST(EPOCH(s.timestamp) AS BIGINT) * 1000
                    + ds.server_key AS telemetry_key

            FROM silver_telemetry s

            JOIN dim_server ds
                ON s.server_id = ds.server_id

            JOIN dim_datacenter dc
                ON s.datacenter_id = dc.datacenter_id

            JOIN dim_date dd
                ON s.telemetry_date = dd.full_date
        """)

        # ---------------------------------------
        # 1. Populate fact_telemetry
        # ---------------------------------------

        connection.execute("""
            INSERT INTO fact_telemetry (
                telemetry_key,
                server_key,
                datacenter_key,
                date_key,
                timestamp,
                telemetry_hour,
                cpu_utilization,
                memory_utilization,
                disk_utilization,
                network_in_mbps,
                network_out_mbps,
                temperature,
                power_consumption,
                fan_speed_rpm,
                server_status,
                is_anomaly
            )
            SELECT
                e.telemetry_key,
                e.server_key,
                e.datacenter_key,
                e.date_key,
                e.timestamp,
                e.telemetry_hour,
                e.cpu_utilization,
                e.memory_utilization,
                e.disk_utilization,
                e.network_in_mbps,
                e.network_out_mbps,
                e.temperature,
                e.power_consumption,
                e.fan_speed_rpm,
                e.server_status,
                e.is_anomaly
            FROM enriched_telemetry e
            WHERE NOT EXISTS (
                SELECT 1
                FROM fact_telemetry f
                WHERE f.telemetry_key = e.telemetry_key
            )
        """)

        logger.info("Telemetry fact table populated.")

        # ---------------------------------------
        # 2. Populate fact_alerts
        # ---------------------------------------

        connection.execute("""
            INSERT INTO fact_alerts (
                alert_key,
                server_key,
                datacenter_key,
                date_key,
                timestamp,
                anomaly_type,
                anomaly_severity,
                cpu_utilization,
                memory_utilization,
                disk_utilization,
                temperature,
                power_consumption,
                server_status
            )
            SELECT
                e.telemetry_key,
                e.server_key,
                e.datacenter_key,
                e.date_key,
                e.timestamp,
                e.anomaly_type,
                e.anomaly_severity,
                e.cpu_utilization,
                e.memory_utilization,
                e.disk_utilization,
                e.temperature,
                e.power_consumption,
                e.server_status
            FROM enriched_telemetry e
            WHERE e.is_anomaly = TRUE
              AND NOT EXISTS (
                  SELECT 1
                  FROM fact_alerts f
                  WHERE f.alert_key = e.telemetry_key
              )
        """)

        logger.info("Alert fact table populated.")

        # ---------------------------------------
        # 3. Validate fact table counts
        # ---------------------------------------

        expected_telemetry = connection.execute("""
            SELECT COUNT(*) FROM enriched_telemetry
        """).fetchone()[0]

        expected_alerts = connection.execute("""
            SELECT COUNT(*)
            FROM enriched_telemetry
            WHERE is_anomaly = TRUE
        """).fetchone()[0]

        actual_telemetry = connection.execute("""
            SELECT COUNT(*) FROM fact_telemetry
        """).fetchone()[0]

        actual_alerts = connection.execute("""
            SELECT COUNT(*) FROM fact_alerts
        """).fetchone()[0]

        logger.info(
            "Gold fact validation | expected_telemetry=%s | actual_telemetry=%s | expected_alerts=%s | actual_alerts=%s",
            expected_telemetry,
            actual_telemetry,
            expected_alerts,
            actual_alerts,
        )

        assert actual_telemetry == expected_telemetry
        assert actual_alerts == expected_alerts

        # ---------------------------------------
        # 4. Check foreign key mappings
        # ---------------------------------------

        orphan_telemetry = connection.execute("""
            SELECT COUNT(*)
            FROM fact_telemetry f
            LEFT JOIN dim_server s
                ON f.server_key = s.server_key
            LEFT JOIN dim_datacenter d
                ON f.datacenter_key = d.datacenter_key
            LEFT JOIN dim_date dt
                ON f.date_key = dt.date_key
            WHERE
                s.server_key IS NULL
                OR d.datacenter_key IS NULL
                OR dt.date_key IS NULL
        """).fetchone()[0]

        orphan_alerts = connection.execute("""
            SELECT COUNT(*)
            FROM fact_alerts f
            LEFT JOIN dim_server s
                ON f.server_key = s.server_key
            LEFT JOIN dim_datacenter d
                ON f.datacenter_key = d.datacenter_key
            LEFT JOIN dim_date dt
                ON f.date_key = dt.date_key
            WHERE
                s.server_key IS NULL
                OR d.datacenter_key IS NULL
                OR dt.date_key IS NULL
        """).fetchone()[0]

        logger.info(
            "Orphan records | telemetry=%s | alerts=%s",
            orphan_telemetry,
            orphan_alerts,
        )

        assert orphan_telemetry == 0
        assert orphan_alerts == 0

        connection.execute("COMMIT")

        logger.info("Gold fact population completed successfully.")

    except Exception:
        logger.exception("Gold fact population failed. Rolling back transaction.")
        connection.execute("ROLLBACK")
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    populate_facts()