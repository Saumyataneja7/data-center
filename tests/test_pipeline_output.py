from pathlib import Path

import pandas as pd


PARQUET_FILE = Path(
    "data/raw/telemetry/date=2026-11-25/batch=1830.parquet"
)


def test_pipeline_output():
    # Check that the Parquet file exists.
    assert PARQUET_FILE.exists(), "Parquet file not found!"

    # Read the generated data.
    df = pd.read_parquet(PARQUET_FILE, engine="pyarrow")

    print("\n--- PARQUET VALIDATION REPORT ---")

    # 1. Record count
    print(f"Total records: {len(df)}")
    assert len(df) == 500

    # 2. Unique servers
    unique_servers = df["server_id"].nunique()
    print(f"Unique servers: {unique_servers}")
    assert unique_servers == 500

    # 3. Required columns
    required_columns = [
        "server_id",
        "timestamp",
        "is_anomaly",
        "anomaly_type",
    ]

    for column in required_columns:
        assert column in df.columns, f"Missing column: {column}"

    print(f"Total columns: {len(df.columns)}")
    print(f"Columns: {list(df.columns)}")

    # 4. Timestamp validation
    print(f"Unique timestamps: {df['timestamp'].nunique()}")
    print(f"Timestamp data type: {df['timestamp'].dtype}")

    assert df["timestamp"].nunique() == 1
    assert isinstance(df["timestamp"].dtype, pd.DatetimeTZDtype)

    # 5. Duplicate validation
    duplicates = df["server_id"].duplicated().sum()
    print(f"Duplicate server records: {duplicates}")
    assert duplicates == 0

    # 6. Anomaly validation
    anomaly_count = int(df["is_anomaly"].sum())
    anomaly_rate = anomaly_count / len(df) * 100

    print(f"Anomalies: {anomaly_count}")
    print(f"Anomaly rate: {anomaly_rate:.2f}%")

    assert 3 <= anomaly_rate <= 5

    # 7. Anomaly distribution
    print("\nAnomaly distribution:")
    print(
        df.loc[df["is_anomaly"], "anomaly_type"]
        .value_counts()
    )

    # 8. Missing values
    print("\nMissing values by column:")
    print(df.isnull().sum())

    print("\nParquet validation passed!")


if __name__ == "__main__":
    test_pipeline_output()