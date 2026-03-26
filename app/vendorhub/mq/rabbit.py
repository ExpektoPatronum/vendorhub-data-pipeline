import json
from datetime import datetime, timezone

import pika

from vendorhub.config import load_config


def rmq_connection(cfg):
    creds = pika.PlainCredentials(cfg.rmq.user, cfg.rmq.password)
    params = pika.ConnectionParameters(
        host=cfg.rmq.host,
        port=cfg.rmq.port,
        credentials=creds,
        heartbeat=30,
        blocked_connection_timeout=60,
    )
    return pika.BlockingConnection(params)


def ensure_exchange(channel, cfg):
    channel.exchange_declare(
        exchange=cfg.rmq.exchange,
        exchange_type="direct",
        durable=True,
    )


def ensure_vendor_queues(channel, cfg, vendor_code: str):

    if vendor_code not in cfg.rmq.vendor_queues:
        raise RuntimeError(f"Unknown vendor_code={vendor_code}. Check VENDORS in .env")

    qcfg = cfg.rmq.vendor_queues[vendor_code]

    # exchange
    ensure_exchange(channel, cfg)

    # DLQ
    channel.queue_declare(queue=qcfg.dlq, durable=True)

    # main queue (с DLX -> exchange и routing-key -> dlq)
    channel.queue_declare(
        queue=qcfg.queue,
        durable=True,
        arguments={
            "x-dead-letter-exchange": cfg.rmq.exchange,
            "x-dead-letter-routing-key": qcfg.dlq,
        },
    )

    # bind main queue to routing key
    channel.queue_bind(queue=qcfg.queue, exchange=cfg.rmq.exchange, routing_key=qcfg.routing_key)
    channel.basic_qos(prefetch_count=10)

def publish_raw_ingested(channel, cfg, vendor_code: str, raw_id: int):
    """
    Публикация события в exchange. Producer использует это.
    """
    if vendor_code not in cfg.rmq.vendor_queues:
        raise RuntimeError(f"Unknown vendor_code={vendor_code}. Check VENDORS in .env")

    qcfg = cfg.rmq.vendor_queues[vendor_code]

    event = {
        "event_type": "raw_ingested",
        "vendor_code": vendor_code,
        "raw_id": int(raw_id),
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    channel.basic_publish(
        exchange=cfg.rmq.exchange,
        routing_key=qcfg.routing_key,
        body=json.dumps(event).encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,  # persistent
        ),
        # mandatory=True можно включить позже, когда ты хочешь ловить Unroutable
        mandatory=False,
    )


def open_channel():
    """
    Удобная функция: открывает соединение и канал.
    """
    cfg = load_config()
    conn = rmq_connection(cfg)
    ch = conn.channel()
    return cfg, conn, ch