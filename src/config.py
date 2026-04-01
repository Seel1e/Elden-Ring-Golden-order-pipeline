"""
Central configuration — reads from .env file or environment variables.
All pipeline components import from here to avoid scattered magic strings.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)


class WarehouseConfig:
    HOST: str = os.getenv("WAREHOUSE_HOST", "localhost")
    PORT: int = int(os.getenv("WAREHOUSE_PORT", "5432"))
    USER: str = os.getenv("WAREHOUSE_USER", "eruser")
    PASSWORD: str = os.getenv("WAREHOUSE_PASSWORD", "erpass")
    DB: str = os.getenv("WAREHOUSE_DB", "elden_ring_dw")

    @classmethod
    def dsn(cls) -> str:
        return (
            f"postgresql+psycopg2://{cls.USER}:{cls.PASSWORD}"
            f"@{cls.HOST}:{cls.PORT}/{cls.DB}"
        )

    @classmethod
    def jdbc_url(cls) -> str:
        """JDBC URL used by PySpark to write to the warehouse."""
        return f"jdbc:postgresql://{cls.HOST}:{cls.PORT}/{cls.DB}"

    @classmethod
    def jdbc_props(cls) -> dict:
        return {"user": cls.USER, "password": cls.PASSWORD, "driver": "org.postgresql.Driver"}


class KafkaConfig:
    BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_ENCOUNTERS: str = os.getenv("KAFKA_TOPIC_ENCOUNTERS", "player_encounters")
    TOPIC_DLQ: str = os.getenv("KAFKA_TOPIC_DLQ", "player_encounters_dlq")
    CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "er_spark_consumer")


class APIConfig:
    BASE_URL: str = os.getenv("ELDENRING_API_BASE_URL", "https://eldenring.fanapis.com/api")
    PAGE_SIZE: int = int(os.getenv("ELDENRING_API_PAGE_SIZE", "100"))
    REQUEST_DELAY: float = float(os.getenv("ELDENRING_API_REQUEST_DELAY_S", "0.25"))
    # Endpoints available on the fan API
    ENDPOINTS: list[str] = ["weapons", "bosses", "armors", "shields", "spells"]


class DataLakeConfig:
    ROOT: Path = Path(os.getenv("DATA_LAKE_ROOT", str(_ROOT / "data" / "raw")))
    STAGING: Path = Path(os.getenv("STAGING_ROOT", str(_ROOT / "data" / "staged")))
    KAGGLE_DIR: Path = _ROOT / "eldenringScrap"


class TelemetryConfig:
    EVENTS_PER_SECOND: int = int(os.getenv("TELEMETRY_EVENTS_PER_SECOND", "500"))
    NUM_PLAYERS: int = int(os.getenv("TELEMETRY_NUM_PLAYERS", "10000"))
    RUN_DURATION_S: int = int(os.getenv("TELEMETRY_RUN_DURATION_S", "0"))
