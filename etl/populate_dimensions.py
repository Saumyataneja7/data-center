from pathlib import Path

import duckdb

from utils.logger import get_logger

logger = get_logger("gold_dimensions", "gold_dimensions.log")


PROJECT_ROOT = Path(__file__).resolve().parent.parent

GOLD_DB = PROJECT_ROOT / "database" / "data_center_gold.duckdb"
SILVER_GLOB = str(
    PROJECT_ROOT / "data" / "silver" / "telemetry" / "**" / "*.parquet"
)


def populate_dimensions():

    connection = duckdb.connect(str(GOLD_DB))

    try:
        # Read Silver telemetry.
        connection.execute(f"""
            CREATE OR REPLACE VIEW silver_telemetry AS
            SELECT *
            FROM read_parquet('{SILVER_GLOB}')
        """)

        # ---------------------------------------
        # 1. Populate dim_datacenter
        # ---------------------------------------

        connection.execute("""
            INSERT INTO dim_datacenter
            SELECT
                ROW_NUMBER() OVER (ORDER BY datacenter_id) AS datacenter_key,
                datacenter_id,
                datacenter_name,
                city,
                country
            FROM (
                SELECT DISTINCT
                    datacenter_id,
                    datacenter_name,
                    city,
                    country
                FROM silver_telemetry
            ) d
            WHERE NOT EXISTS (
                SELECT 1
                FROM dim_datacenter existing
                WHERE existing.datacenter_id = d.datacenter_id
            )
        """)

        logger.info("Data center dimension populated.")

        # ---------------------------------------
        # 2. Populate dim_server
        # ---------------------------------------

        connection.execute("""
            INSERT INTO dim_server
            SELECT
                ROW_NUMBER() OVER (ORDER BY s.server_id)
                    + COALESCE((SELECT MAX(server_key) FROM dim_server), 0)
                    AS server_key,
                s.server_id,
                d.datacenter_key,
                s.rack_id,
                s.server_type,
                s.operating_system,
                s.cpu_cores,
                s.memory_gb
            FROM (
                SELECT DISTINCT
                    server_id,
                    datacenter_id,
                    rack_id,
                    server_type,
                    operating_system,
                    cpu_cores,
                    memory_gb
                FROM silver_telemetry
            ) s
            JOIN dim_datacenter d
                ON s.datacenter_id = d.datacenter_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM dim_server existing
                WHERE existing.server_id = s.server_id
            )
        """)

        logger.info("Server dimension populated.")

        # ---------------------------------------
        # 3. Populate dim_date
        # ---------------------------------------

        connection.execute("""
            INSERT INTO dim_date
            SELECT
                CAST(STRFTIME(d, '%Y%m%d') AS INTEGER) AS date_key,
                d AS full_date,
                EXTRACT(DAY FROM d) AS day,
                EXTRACT(MONTH FROM d) AS month,
                STRFTIME(d, '%B') AS month_name,
                EXTRACT(QUARTER FROM d) AS quarter,
                EXTRACT(YEAR FROM d) AS year,
                STRFTIME(d, '%A') AS day_of_week,
                CASE
                    WHEN EXTRACT(DOW FROM d) IN (0, 6)
                    THEN TRUE
                    ELSE FALSE
                END AS is_weekend
            FROM (
                SELECT DISTINCT telemetry_date AS d
                FROM silver_telemetry
            ) dates
            WHERE NOT EXISTS (
                SELECT 1
                FROM dim_date existing
                WHERE existing.full_date = d
            )
        """)

        logger.info("Date dimension populated.")

        # ---------------------------------------
        # 4. Validate dimensions
        # ---------------------------------------

        counts = connection.execute("""
            SELECT
                (SELECT COUNT(*) FROM dim_datacenter),
                (SELECT COUNT(*) FROM dim_server),
                (SELECT COUNT(*) FROM dim_date)
        """).fetchone()

        logger.info(
            "Dimension counts | data_centers=%s | servers=%s | dates=%s",
            counts[0],
            counts[1],
            counts[2],
        )

        # Check server-to-datacenter mapping.
        orphan_servers = connection.execute("""
            SELECT COUNT(*)
            FROM dim_server s
            LEFT JOIN dim_datacenter d
                ON s.datacenter_key = d.datacenter_key
            WHERE d.datacenter_key IS NULL
        """).fetchone()[0]

        logger.info("Servers without a data center | count=%s", orphan_servers)

        assert counts[0] == 5
        assert counts[1] == 500
        assert orphan_servers == 0

        logger.info("Dimension population completed successfully.")

    except Exception:
        logger.exception("Gold dimension population failed.")
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    populate_dimensions()