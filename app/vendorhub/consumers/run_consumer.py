import json
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import pandas as pd
import pika
from sqlalchemy import bindparam, text

from vendorhub.config import load_config
from vendorhub.db.session import SessionLocal
from vendorhub.db.staging_repo import insert_rejected_rows, fetch_raw_rows
from vendorhub.db.core_repo import fetch_existing_prices, upsert_products_batch, load_vendor_map, load_currency_map, insert_price_history_batch
from vendorhub.mq.rabbit import rmq_connection, ensure_vendor_queues
from vendorhub.transforms.normalizers.vendor_a import normalize_vendor_a
from vendorhub.transforms.normalizers.vendor_b import normalize_vendor_b
from vendorhub.transforms.validate_unified import validate_unified


VENDOR_NORMALIZERS = {
        "vendor_a": normalize_vendor_a,
        "vendor_b": normalize_vendor_b,
    }

logger = logging.getLogger(__name__)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def normalize_batch(vendor_code: str, df: pd.DataFrame) -> pd.DataFrame:
    payload_df = pd.json_normalize(df["payload"])  #вернет новый dataframe поэтому df.copy() не нужен
    normalize_fn = VENDOR_NORMALIZERS.get(vendor_code)

    if normalize_fn is None:
        raise ValueError(f"Unknown vendor_code={vendor_code}")

    out = normalize_fn(payload_df)
    out["raw_id"] = df["raw_id"].astype("int64").to_numpy() # to_numpy() нжуен чтобы присобачить raw_id к каждой строке по позиции а не по индексу
    out["vendor_code"] = vendor_code

    return out

# ---------- RabbitMQ batch pull ----------
def pull_batch(ch, queues: List[str], batch_size: int, max_wait_sec: float) -> List[Tuple[str, Any, Any, bytes]]:
    """
    Забираем пачку сообщений из нескольких очередей через basic_get.
    Возвращаем список (queue, method, props, body).
    """
    items = []
    started = time.time()

    while len(items) < batch_size and (time.time() - started) < max_wait_sec:
        got_any = False
        for q in queues:
            method, props, body = ch.basic_get(queue=q, auto_ack=False)
            if method is not None:
                items.append((q, method, props, body))
                got_any = True
                if len(items) >= batch_size:
                    break
        if not got_any:
            time.sleep(0.2)

    return items


def main():
    setup_logging()
    cfg = load_config()

    conn = rmq_connection(cfg)
    ch = conn.channel()
    ch.basic_qos(prefetch_count=50)

    # ensure infra + список очередей
    queues = []
    for vendor_code, qcfg in cfg.rmq.vendor_queues.items():
        ensure_vendor_queues(ch, cfg, vendor_code)
        queues.append(qcfg.queue)

    logger.info("Batch consumer listening queues: %s", queues)

    batch_size = 100
    max_wait_sec = 3.0

    # кэш vendor_code -> vendor_id
    with SessionLocal() as session:
        vendor_map = load_vendor_map(session)
        currency_map = load_currency_map(session)
    logger.info("Vendor and currency map loaded: %s, %s", vendor_map, currency_map)

    while True:
        items = pull_batch(ch, queues=queues, batch_size=batch_size, max_wait_sec=max_wait_sec)
        if not items:
            continue

        events = []
        delivery_tags = []

        for _, method, props, body in items:        # распарсим события
            try:
                event = json.loads(body.decode("utf-8"))
                events.append(event)
                delivery_tags.append(method.delivery_tag)
            except Exception:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)   # битое сообщение -> в DLQ

        raw_ids = [int(e["raw_id"]) for e in events] # raw_ids для одной DB выборки

        session = SessionLocal()
        try:
            raw_rows = fetch_raw_rows(session, raw_ids)
            if not raw_rows:
                # нечего обрабатывать -> ack, чтобы не зависло
                for tag in delivery_tags:
                    ch.basic_ack(delivery_tag=tag)
                session.commit()
                continue

            raw_df = pd.DataFrame(raw_rows)

            # Группируем по vendor_code и нормализуем пачками по вендору
            unified_valid_frames = []
            unified_invalid_frames = []

            for vendor_code, part in raw_df.groupby("vendor_code"):
                u = normalize_batch(str(vendor_code), part)
                valid_df, invalid_df = validate_unified(u)
                unified_valid_frames.append(valid_df)
                unified_invalid_frames.append(invalid_df)

            if not unified_valid_frames or all(df.empty for df in unified_valid_frames):
                for tag in delivery_tags:
                    ch.basic_ack(delivery_tag=tag)
                session.commit()
                continue

            unified_df = pd.concat(unified_valid_frames, ignore_index=True)

            if unified_invalid_frames and any(not df.empty for df in unified_invalid_frames):
                rejected_df = pd.concat(unified_invalid_frames, ignore_index=True)

                rejected_df = rejected_df.merge(
                    raw_df[["raw_id", "payload"]],
                    on="raw_id",
                    how="left",
                )

                rejected_df = rejected_df[["raw_id", "vendor_code", "error_reason", "payload"]].copy()

                rejected_df["error_reason"] = rejected_df["error_reason"].apply(json.dumps) # из dict в jsonb
                rejected_df["payload"] = rejected_df["payload"].apply(json.dumps)  # делаем из полей dict payload в  jsonb

                rejected_payload = rejected_df.to_dict("records")
                insert_rejected_rows(session, rejected_payload)



            # Для каждого вендора: change detection + upsert + history batch
            for vendor_code, part in unified_df.groupby("vendor_code"):
                vendor_code = str(vendor_code)
                vendor_id = vendor_map.get(vendor_code)
                if not vendor_id:
                    raise RuntimeError(f"vendor_code={vendor_code} not found in core.d_vendor")

                skus = part["vendor_sku"].astype(str).tolist()

                before_map = fetch_existing_prices(session, vendor_id, skus)

                # upsert пачки
                upsert_products_batch(session, vendor_id, part)

                # fetch after (чтобы понять product_id и новую цену)
                after_map = fetch_existing_prices(session, vendor_id, skus)

                hist_rows = []
                for r in part.to_dict(orient="records"):
                    sku = str(r["vendor_sku"])
                    new_price = Decimal(str(r["price"])).quantize(Decimal("0.01"))

                    if sku in before_map:
                        old_pid, old_price = before_map[sku]
                        if old_price is not None and Decimal(old_price) != new_price:
                            pid = after_map.get(sku, (old_pid, None))[0]
                            hist_rows.append(
                                {
                                    "product_id": pid,
                                    "old_price": old_price,
                                    "new_price": new_price,
                                    "raw_id": int(r["raw_id"]),
                                    "vendor_code": vendor_code,
                                }
                            )
                    else:
                        # новый продукт: по желанию можно писать историю как old_price=NULL -> new_price
                        pid = after_map.get(sku, (None, None))[0]
                        if pid is not None:
                            hist_rows.append(
                                {
                                    "product_id": pid,
                                    "old_price": None,
                                    "new_price": new_price,
                                    "raw_id": int(r["raw_id"]),
                                    "vendor_code": vendor_code,
                                }
                            )

                insert_price_history_batch(session, hist_rows)

            session.commit()

            # ack после commit
            for tag in delivery_tags:
                ch.basic_ack(delivery_tag=tag)

            logger.info("Processed batch: %s messages, %s raw rows", len(items), len(raw_df))

        except Exception:
            session.rollback()
            logger.exception("Batch failed -> nack to DLQ")
            # Все сообщения батча в DLQ (важно: requeue=False)
            for _, method, _, _ in items:
                try:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                except Exception:
                    pass
        finally:
            session.close()


if __name__ == "__main__":
    main()