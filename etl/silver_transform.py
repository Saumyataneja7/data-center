from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "telemetry"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver" / "telemetry"


REQUIRED_COLUMNS = [
    "server_id",
    "datacenter_id",
    "timestamp",
    "cpu_utilization",
    "memory_utilization",
    "disk_utilization",
    "network_in_mbps",
    "network_out_mbps",
    "temperature",
    "power_consumption",
    "fan_speed_rpm",
    "server_status",
    "is_anomaly",
    "anomaly_type",
    "anomaly_severity",
]


def validate_batch(connection, input_file):
    """Validate a raw telemetry batch before transformation."""

    file_path = str(input_file).replace("'", "''")

    query = f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT server_id) AS unique_servers,
            COUNT(DISTINCT timestamp) AS unique_timestamps,

            COUNT(*) FILTER (
                WHERE server_id IS NULL
                   OR datacenter_id IS NULL
                   OR timestamp IS NULL
                   OR cpu_utilization IS NULL
                   OR memory_utilization IS NULL
                   OR disk_utilization IS NULL
                   OR server_status IS NULL
                   OR is_anomaly IS NULL
            ) AS missing_required_values,

            COUNT(*) FILTER (
                WHERE cpu_utilization NOT BETWEEN 0 AND 100
                   OR memory_utilization NOT BETWEEN 0 AND 100
                   OR disk_utilization NOT BETWEEN 0 AND 100
                   OR temperature NOT BETWEEN 0 AND 80
                   OR network_in_mbps < 0
                   OR network_out_mbps < 0
                   OR power_consumption < 0
                   OR fan_speed_rpm < 0
            ) AS invalid_metric_records,

            COUNT(*) FILTER (
                WHERE server_status NOT IN ('Healthy', 'Warning', 'Critical')
            ) AS invalid_status_records,

            COUNT(*) FILTER (
                WHERE is_anomaly = TRUE
                  AND (anomaly_type IS NULL OR anomaly_severity IS NULL)
            ) AS incomplete_anomaly_records,

            COUNT(*) FILTER (
                WHERE is_anomaly = FALSE
                  AND (anomaly_type IS NOT NULL OR anomaly_severity IS NOT NULL)
            ) AS inconsistent_normal_records

        FROM read_parquet('{file_path}')
    """

    result = connection.execute(query).fetchone()

    report = {
        "total_records": result[0],
        "unique_servers": result[1],
        "unique_timestamps": result[2],
        "missing_required_values": result[3],
        "invalid_metric_records": result[4],
        "invalid_status_records": result[5],
        "incomplete_anomaly_records": result[6],
        "inconsistent_normal_records": result[7],
    }

    errors = []

    if report["total_records"] != 500:
        errors.append("Expected exactly 500 records.")

    if report["unique_servers"] != report["total_records"]:
        errors.append("Duplicate or missing server records detected.")

    if report["unique_timestamps"] != 1:
        errors.append("Batch contains inconsistent timestamps.")

    for field, value in report.items():
        if field in (
            "missing_required_values",
            "invalid_metric_records",
            "invalid_status_records",
            "incomplete_anomaly_records",
            "inconsistent_normal_records",
        ) and value > 0:
            errors.append(f"{field}: {value}")

    return report, errors


def transform_batch(connection, input_file, output_file):
    """Validate and enrich one raw batch."""

    input_path = str(input_file).replace("'", "''")
    output_path = str(output_file).replace("'", "''")

    report, errors = validate_batch(connection, input_file)

    print(f"\nValidating: {input_file.name}")
    print(f"Records: {report['total_records']}")
    print(f"Unique servers: {report['unique_servers']}")

    if errors:
        raise ValueError(
            "Silver transformation stopped:\n- " + "\n- ".join(errors)
        )

    print("Validation passed.")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    temporary_file = output_file.with_name(
        output_file.stem + ".tmp.parquet"
    )

    if output_file.exists():
        raise FileExistsError(
            f"Silver batch already exists: {output_file}"
        )

    if temporary_file.exists():
        temporary_file.unlink()

    temporary_path = str(temporary_file).replace("'", "''")

    transform_query = f"""
        COPY (
            SELECT
                *,

                CAST(timezone('Asia/Kolkata', timestamp) AS DATE) AS telemetry_date,
                EXTRACT(
                    HOUR FROM timezone('Asia/Kolkata', timestamp)
                ) AS telemetry_hour,

                CASE
                    WHEN cpu_utilization < 40 THEN 'Low'
                    WHEN cpu_utilization < 70 THEN 'Moderate'
                    WHEN cpu_utilization < 85 THEN 'High'
                    ELSE 'Critical'
                END AS cpu_utilization_band,

                CASE
                    WHEN memory_utilization < 50 THEN 'Low'
                    WHEN memory_utilization < 75 THEN 'Moderate'
                    WHEN memory_utilization < 90 THEN 'High'
                    ELSE 'Critical'
                END AS memory_utilization_band,

                CASE
                    WHEN temperature < 30 THEN 'Normal'
                    WHEN temperature < 45 THEN 'Elevated'
                    WHEN temperature < 60 THEN 'High'
                    ELSE 'Critical'
                END AS temperature_band

            FROM read_parquet('{input_path}')
        )
        TO '{temporary_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """

    connection.execute(transform_query)
    temporary_file.replace(output_file)

    print(f"Silver file created: {output_file}")

    return report


def main():
    raw_files = sorted(RAW_DATA_DIR.rglob("*.parquet"))

    if not raw_files:
        print("No raw Parquet files found.")
        return

    connection = duckdb.connect(database=":memory:")

    processed = 0

    try:
        for input_file in raw_files:
            relative_path = input_file.relative_to(RAW_DATA_DIR)
            output_file = SILVER_DATA_DIR / relative_path

            if output_file.exists():
                print(f"\nSkipping existing Silver batch: {output_file.name}")
                continue

            transform_batch(
                connection,
                input_file,
                output_file
            )

            processed += 1

    finally:
        connection.close()

    print("\n========== SILVER TRANSFORMATION SUMMARY ==========")
    print(f"Raw batches discovered: {len(raw_files)}")
    print(f"New batches transformed: {processed}")
    print(f"Silver directory: {SILVER_DATA_DIR}")


if __name__ == "__main__":
    main()