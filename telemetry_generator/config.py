from pathlib import Path

# ==========================================
# PROJECT PATHS
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"

RAW_DATA_DIR = DATA_DIR / "raw" / "telemetry"
METADATA_DIR = DATA_DIR / "metadata"
STATE_DIR = METADATA_DIR / "device_states"

# ==========================================
# DATA CENTER CONFIGURATION
# ==========================================

DATA_CENTERS = [
    {
        "datacenter_id": "DC01",
        "datacenter_name": "Delhi Data Center",
        "city": "Delhi",
        "country": "India",
        "server_count": 100,
    },
    {
        "datacenter_id": "DC02",
        "datacenter_name": "Mumbai Data Center",
        "city": "Mumbai",
        "country": "India",
        "server_count": 100,
    },
    {
        "datacenter_id": "DC03",
        "datacenter_name": "Bengaluru Data Center",
        "city": "Bengaluru",
        "country": "India",
        "server_count": 100,
    },
    {
        "datacenter_id": "DC04",
        "datacenter_name": "Hyderabad Data Center",
        "city": "Hyderabad",
        "country": "India",
        "server_count": 100,
    },
    {
        "datacenter_id": "DC05",
        "datacenter_name": "Chennai Data Center",
        "city": "Chennai",
        "country": "India",
        "server_count": 100,
    },
]

# ==========================================
# SIMULATION SETTINGS
# ==========================================

TOTAL_SERVERS = sum(
    dc["server_count"] for dc in DATA_CENTERS
)

TELEMETRY_INTERVAL_MINUTES = 15

RECORDS_PER_SERVER_PER_DAY = (
    24 * 60 // TELEMETRY_INTERVAL_MINUTES
)

EXPECTED_DAILY_RECORDS = (
    TOTAL_SERVERS * RECORDS_PER_SERVER_PER_DAY
)

# ==========================================
# ANOMALY CONFIGURATION
# ==========================================

ANOMALY_RATE_MIN = 0.03
ANOMALY_RATE_MAX = 0.05

# ==========================================
# REPRODUCIBILITY
# ==========================================

RANDOM_SEED = 42

# ==========================================
# STORAGE SETTINGS
# ==========================================

TELEMETRY_FORMAT = "parquet"

# ==========================================
# VALIDATION
# ==========================================

assert TOTAL_SERVERS == 500
assert RECORDS_PER_SERVER_PER_DAY == 96
assert EXPECTED_DAILY_RECORDS == 48_000

print("Simulation configuration validated!")
print("Total servers:", TOTAL_SERVERS)
print("Records per server per day:", RECORDS_PER_SERVER_PER_DAY)
print("Expected daily records:", EXPECTED_DAILY_RECORDS)