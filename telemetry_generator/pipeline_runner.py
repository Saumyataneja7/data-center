import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from telemetry_generator.config import (
    RAW_DATA_DIR,
    CHECKPOINT_DIR,
    CHECKPOINT_FILE,
    TELEMETRY_INTERVAL_MINUTES,
)
from telemetry_generator.device_simulator import create_device_registry
from telemetry_generator.device_state import DeviceStateManager
from telemetry_generator.telemetry_generator import TelemetryGenerator
from telemetry_generator.anomaly_injector import AnomalyInjector


IST = ZoneInfo("Asia/Kolkata")


class PipelineRunner:

    def __init__(self):
        self.servers = create_device_registry()

        self.state_manager = DeviceStateManager()

        # Restore existing server states instead of resetting them.
        states_loaded = self.state_manager.load_states()

        if not states_loaded:
            print("No previous server states found. Initializing all servers.")
            self.state_manager.initialize_all(self.servers)

        self.telemetry_generator = TelemetryGenerator(
            self.servers,
            self.state_manager
        )

        self.anomaly_injector = AnomalyInjector(self.state_manager)

        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    def get_next_timestamp(self):
        """
        Return the next interval after the last successful run.
        On the first run, use the most recently completed 15-minute interval.
        """

        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, "r") as file:
                checkpoint = json.load(file)

            last_timestamp = datetime.fromisoformat(
                checkpoint["last_successful_timestamp"]
            )

            return last_timestamp + timedelta(
                minutes=TELEMETRY_INTERVAL_MINUTES
            )

        now = datetime.now(IST)

        current_interval_minute = (
            now.minute // TELEMETRY_INTERVAL_MINUTES
        ) * TELEMETRY_INTERVAL_MINUTES

        current_interval = now.replace(
            minute=current_interval_minute,
            second=0,
            microsecond=0
        )

        return current_interval - timedelta(
            minutes=TELEMETRY_INTERVAL_MINUTES
        )

    def save_checkpoint(self, timestamp, record_count, anomaly_count):
        checkpoint = {
            "last_successful_timestamp": timestamp.isoformat(),
            "record_count": record_count,
            "anomaly_count": anomaly_count,
            "total_servers": len(self.servers),
            "status": "SUCCESS"
        }

        temporary_file = CHECKPOINT_FILE.with_suffix(".tmp")

        with open(temporary_file, "w") as file:
            json.dump(checkpoint, file, indent=4)

        temporary_file.replace(CHECKPOINT_FILE)

    def save_parquet(self, records, timestamp):
        date_folder = RAW_DATA_DIR / f"date={timestamp:%Y-%m-%d}"
        date_folder.mkdir(parents=True, exist_ok=True)

        file_name = f"batch={timestamp:%H%M}.parquet"
        output_path = date_folder / file_name

        if output_path.exists():
            raise FileExistsError(
                f"Batch already exists: {output_path}"
            )

        dataframe = pd.DataFrame(records)

        # Store timestamps as proper datetime values in Parquet.
        dataframe["timestamp"] = pd.to_datetime(
            dataframe["timestamp"],
            utc=True
        )

        temporary_path = output_path.with_name(
            output_path.stem + ".tmp.parquet"
        )

        dataframe.to_parquet(
            temporary_path,
            engine="pyarrow",
            index=False
        )

        # Rename only after the Parquet write succeeds.
        temporary_path.replace(output_path)

        return output_path

    def run_once(self, timestamp=None):
        if timestamp is None:
            timestamp = self.get_next_timestamp()

        if timestamp.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware.")

        if (
            timestamp.minute % TELEMETRY_INTERVAL_MINUTES != 0
            or timestamp.second != 0
            or timestamp.microsecond != 0
        ):
            raise ValueError(
                "Timestamp must align with a 15-minute interval."
            )

        print("\nStarting Data Center Telemetry Pipeline")
        print(f"Timestamp: {timestamp.isoformat()}")
        print(f"Servers: {len(self.servers)}")

        # 1. Generate normal telemetry.
        records = self.telemetry_generator.generate_batch(timestamp)

        # 2. Inject anomalies.
        records = self.anomaly_injector.inject_anomalies(
            records,
            timestamp
        )

        anomaly_count = sum(
            1 for record in records if record["is_anomaly"]
        )

        # 3. Save the batch to Parquet.
        output_path = self.save_parquet(records, timestamp)

        # 4. Persist updated server states.
        self.state_manager.save_states()

        # 5. Update the checkpoint after the data and states are saved.
        self.save_checkpoint(
            timestamp,
            len(records),
            anomaly_count
        )

        print("\nPipeline completed successfully.")
        print(f"Records generated: {len(records)}")
        print(f"Anomalies injected: {anomaly_count}")
        print(f"Parquet file: {output_path}")
        print(f"Checkpoint: {CHECKPOINT_FILE}")

        return output_path


if __name__ == "__main__":
    runner = PipelineRunner()
    runner.run_once()