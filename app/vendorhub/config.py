import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


def must_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str


@dataclass(frozen=True)
class VendorQueueConfig:
    queue: str
    routing_key: str
    dlq: str


@dataclass(frozen=True)
class RabbitConfig:
    host: str
    port: int
    user: str
    password: str
    exchange: str
    vendor_queues: Dict[str, VendorQueueConfig]


@dataclass(frozen=True)
class AppConfig:
    pg: PostgresConfig
    rmq: RabbitConfig

    staging_retention_days: int

    inbox_dir: Path
    archive_dir: Path


def _load_vendor_queue(vendor_code: str) -> VendorQueueConfig:
    suffix = vendor_code.upper()
    suffix = suffix.replace("-", "_")

    q = os.getenv(f"RMQ_Q_{suffix}", f"q.{vendor_code}.raw")
    rk = os.getenv(f"RMQ_RK_{suffix}", f"{vendor_code}.raw_ingested")
    dlq = os.getenv(f"RMQ_DLQ_{suffix}", f"q.{vendor_code}.raw.dlq")
    return VendorQueueConfig(queue=q, routing_key=rk, dlq=dlq)


def load_config() -> AppConfig:
    # --- Postgres ---
    pg = PostgresConfig(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=env_int("PG_PORT", 5433),
        db=must_env("POSTGRES_DB"),
        user=must_env("POSTGRES_USER"),
        password=must_env("POSTGRES_PASSWORD"),
    )

    # --- RabbitMQ ---
    rmq_host = os.getenv("RMQ_HOST", "127.0.0.1")
    rmq_port = env_int("RMQ_PORT", 5672)
    rmq_user = must_env("RABBITMQ_DEFAULT_USER")
    rmq_password = must_env("RABBITMQ_DEFAULT_PASS")
    exchange = os.getenv("RMQ_EXCHANGE", "vendor.events")

    # список вендоров, для которых мы хотим иметь отдельные очереди
    # пример: VENDORS=vendor_a, vendor_b
    vendors_raw = os.getenv("VENDORS", "vendor_a,vendor_b")
    vendors = [v.strip() for v in vendors_raw.split(",") if v.strip()]

    vendor_queues: Dict[str, VendorQueueConfig] = {
        v: _load_vendor_queue(v) for v in vendors
    }

    rmq = RabbitConfig(
        host=rmq_host,
        port=rmq_port,
        user=rmq_user,
        password=rmq_password,
        exchange=exchange,
        vendor_queues=vendor_queues,
    )

    staging_retention_days = env_int("STAGING_RETENTION_DAYS", 30)

    inbox_dir = Path(os.getenv("INBOX_DIR", "/opt/vendor-sync/data/inbox"))
    archive_dir = Path(os.getenv("ARCHIVE_DIR", "/opt/vendor-sync/data/archive"))

    return AppConfig(
        pg=pg,
        rmq=rmq,
        staging_retention_days=staging_retention_days,
        inbox_dir=inbox_dir,
        archive_dir=archive_dir,
    )