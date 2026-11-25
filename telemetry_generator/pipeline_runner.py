import copy
import json
import os

from datetime import datetime
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

from scheduler.interval_manager import get_pending_intervals
from scheduler.process_lock import exclusive_process_lock


IST = ZoneInfo("Asia/Kolkata")


class PipelineRunner:

    def __init__(self):

        self.servers = create_device_registry()

        self.state_manager = DeviceStateManager()

        states_loaded = self.state_manager.load_states()

        if not states_loaded:
            print("No previous server states found. Initializing all servers.")
            self.state_manager.initialize_all(self.servers)

        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Recover an interrupted transaction before
        # creating the telemetry and anomaly generators.
        self.recover_pending_transaction()

        self.telemetry_generator = TelemetryGenerator(
            self.servers,
            self.state_manager
        )

        self.anomaly_injector = AnomalyInjector(
            self.state_manager
        )

    @property
    def pending_transaction_file(self):
        return CHECKPOINT_DIR / "pending_transaction.json"

    # ==========================================
    # CHECKPOINT MANAGEMENT
    # ==========================================

    def get_last_successful_timestamp(self):

        if not CHECKPOINT_FILE.exists():
            return None

        with open(CHECKPOINT_FILE, "r") as file:
            checkpoint = json.load(file)

        timestamp = datetime.fromisoformat(
            checkpoint["last_successful_timestamp"]
        )

        if timestamp.tzinfo is None:
            raise ValueError(
                "Checkpoint timestamp must be timezone-aware."
            )

        return timestamp

    def save_checkpoint(
        self,
        timestamp,
        record_count,
        anomaly_count
    ):

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
            file.flush()
            os.fsync(file.fileno())

        temporary_file.replace(CHECKPOINT_FILE)

        print(f"Checkpoint updated: {timestamp.isoformat()}")

    # ==========================================
    # FILE PATHS
    # ==========================================

    def get_batch_path(self, timestamp):

        date_folder = (
            RAW_DATA_DIR / f"date={timestamp:%Y-%m-%d}"
        )

        return date_folder / f"batch={timestamp:%H%M}.parquet"

    # ==========================================
    # PARQUET MANAGEMENT
    # ==========================================

    def save_parquet(self, records, timestamp):

        output_path = self.get_batch_path(timestamp)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        dataframe = pd.DataFrame(records)

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

        # Atomic replacement also supports deterministic
        # replay when an orphan batch already exists.
        temporary_path.replace(output_path)

        return output_path

    def validate_batch(self, batch_path, timestamp):

        if not batch_path.exists():
            raise FileNotFoundError(
                f"Recovery batch is missing: {batch_path}"
            )

        dataframe = pd.read_parquet(batch_path)

        if len(dataframe) != len(self.servers):
            raise ValueError(
                f"Invalid recovery batch record count: {len(dataframe)}"
            )

        if dataframe["server_id"].nunique() != len(self.servers):
            raise ValueError(
                "Recovery batch does not contain unique server readings."
            )

        timestamps = pd.to_datetime(
            dataframe["timestamp"],
            utc=True
        ).drop_duplicates()

        if len(timestamps) != 1:
            raise ValueError(
                "Recovery batch must contain exactly one timestamp."
            )

        expected_timestamp = pd.Timestamp(
            timestamp.astimezone(ZoneInfo("UTC"))
        )

        if timestamps.iloc[0] != expected_timestamp:
            raise ValueError(
                "Recovery batch timestamp does not match transaction."
            )

        return len(dataframe)

    # ==========================================
    # PENDING TRANSACTION MANAGEMENT
    # ==========================================

    def save_pending_transaction(
        self,
        timestamp,
        record_count,
        anomaly_count
    ):

        transaction = {
            "timestamp": timestamp.isoformat(),
            "record_count": record_count,
            "anomaly_count": anomaly_count,
            "total_servers": len(self.servers),
            "states": copy.deepcopy(self.state_manager.states)
        }

        pending_file = self.pending_transaction_file

        temporary_file = pending_file.with_suffix(".tmp")

        with open(temporary_file, "w") as file:
            json.dump(transaction, file, indent=4)
            file.flush()
            os.fsync(file.fileno())

        temporary_file.replace(pending_file)

        print("Pending transaction saved.")

    def recover_pending_transaction(self):

        pending_file = self.pending_transaction_file

        if not pending_file.exists():
            print("No pending transaction to recover.")
            return False

        print("\n========== TRANSACTION RECOVERY ==========")

        with open(pending_file, "r") as file:
            transaction = json.load(file)

        timestamp = datetime.fromisoformat(
            transaction["timestamp"]
        )

        if timestamp.tzinfo is None:
            raise ValueError(
                "Pending transaction timestamp must be timezone-aware."
            )

        # If the checkpoint already includes this transaction,
        # only the cleanup step remains.
        last_successful = self.get_last_successful_timestamp()

        if (
            last_successful is not None
            and timestamp <= last_successful
        ):
            print(
                "Transaction is already committed. "
                "Removing leftover pending file."
            )

            pending_file.unlink()
            return True

        if transaction["total_servers"] != len(self.servers):
            raise ValueError(
                "Pending transaction server count does not match registry."
            )

        states = transaction.get("states")

        if (
            not isinstance(states, dict)
            or len(states) != len(self.servers)
        ):
            raise ValueError(
                "Pending transaction contains an invalid state snapshot."
            )

        batch_path = self.get_batch_path(timestamp)

        actual_record_count = self.validate_batch(
            batch_path,
            timestamp
        )

        if actual_record_count != transaction["record_count"]:
            raise ValueError(
                "Pending transaction record count does not match Parquet."
            )

        print(f"Recovering interval: {timestamp.isoformat()}")

        # Restore the state snapshot associated with the Parquet batch.
        self.state_manager.restore_states(states)

        # Persist restored states before advancing the checkpoint.
        self.state_manager.save_states()

        self.save_checkpoint(
            timestamp,
            transaction["record_count"],
            transaction["anomaly_count"]
        )

        pending_file.unlink()

        print("Pending transaction successfully recovered.")

        return True

    # ==========================================
    # TELEMETRY EXECUTION
    # ==========================================

    def run_once(self, timestamp=None):

        if timestamp is None:
            timestamp = datetime.now(IST)

        if timestamp.tzinfo is None:
            raise ValueError(
                "Timestamp must be timezone-aware."
            )

        timestamp = timestamp.astimezone(IST)

        if (
            timestamp.minute % TELEMETRY_INTERVAL_MINUTES != 0
            or timestamp.second != 0
            or timestamp.microsecond != 0
        ):
            raise ValueError(
                "Timestamp must align with a 15-minute interval."
            )

        last_successful = self.get_last_successful_timestamp()

        if (
            last_successful is not None
            and timestamp <= last_successful
        ):
            raise ValueError(
                f"Interval {timestamp} is already committed."
            )

        print("\nStarting Data Center Telemetry Pipeline")
        print(f"Timestamp: {timestamp.isoformat()}")
        print(f"Servers: {len(self.servers)}")

        # Keep a copy so ordinary Python exceptions can roll back
        # in-memory changes if the transaction has not been committed.
        previous_states = copy.deepcopy(
            self.state_manager.states
        )

        try:

            # 1. Generate normal telemetry.
            records = self.telemetry_generator.generate_batch(
                timestamp
            )

            # 2. Inject deterministic anomalies.
            records = self.anomaly_injector.inject_anomalies(
                records,
                timestamp
            )

            anomaly_count = sum(
                1 for record in records
                if record["is_anomaly"]
            )

            # 3. Write Parquet atomically.
            output_path = self.save_parquet(
                records,
                timestamp
            )

            # 4. Write the recovery record.
            self.save_pending_transaction(
                timestamp,
                len(records),
                anomaly_count
            )

            # 5. Persist device states.
            self.state_manager.save_states()

            # 6. Commit the checkpoint.
            self.save_checkpoint(
                timestamp,
                len(records),
                anomaly_count
            )

            # 7. Remove recovery record after successful commit.
            self.pending_transaction_file.unlink()

            print("\nPipeline completed successfully.")
            print(f"Records generated: {len(records)}")
            print(f"Anomalies injected: {anomaly_count}")
            print(f"Parquet file: {output_path}")
            print(f"Checkpoint: {CHECKPOINT_FILE}")

            return output_path

        except Exception:

            # If a pending transaction exists, leave it intact:
            # the next process can recover it.
            #
            # Otherwise, restore in-memory state. An orphan Parquet
            # batch can be deterministically replayed on restart.
            if not self.pending_transaction_file.exists():
                self.state_manager.restore_states(
                    previous_states
                )

            raise

    # ==========================================
    # INTERVAL RECOVERY
    # ==========================================

    def run_pending_intervals(self, current_timestamp=None):

        if current_timestamp is None:
            current_timestamp = datetime.now(IST)

        last_successful = self.get_last_successful_timestamp()

        pending_intervals = get_pending_intervals(
            last_successful_timestamp=last_successful,
            current_timestamp=current_timestamp,
            interval_minutes=TELEMETRY_INTERVAL_MINUTES
        )

        if not pending_intervals:
            print("\nNo pending telemetry intervals.")
            return 0

        print("\n========== PIPELINE RECOVERY ==========")
        print(f"Last successful interval: {last_successful}")
        print(f"Current time: {current_timestamp}")
        print(f"Pending intervals: {len(pending_intervals)}")

        for index, timestamp in enumerate(
            pending_intervals,
            start=1
        ):

            print(
                f"\nProcessing interval {index}/"
                f"{len(pending_intervals)}"
            )

            self.run_once(timestamp)

        print("\nAll pending intervals processed successfully.")

        return len(pending_intervals)


if __name__ == "__main__":

    lock_path = CHECKPOINT_DIR / "pipeline.lock"

    with exclusive_process_lock(lock_path) as lock_acquired:

        if not lock_acquired:
            raise SystemExit(0)

        runner = PipelineRunner()
        runner.run_pending_intervals()