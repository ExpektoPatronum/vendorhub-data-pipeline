import requests

from vendorhub.db.session import SessionLocal
from vendorhub.db.staging_repo import insert_raw_product

from vendorhub.mq.rabbit import open_channel, publish_raw_ingested

VENDOR_CODE = "vendor_a"


def fetch_vendor_a_products():
    url = "https://fakestoreapi.com/products"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    for item in data:
        yield {
            "vendor_sku": str(item["id"]),
            "title": item["title"],
            "price": float(item["price"]),
            "currency": "USD",
            "category": item.get("category"),
            "brand": None,
            "raw_source": "fakestoreapi",
        }


def main():
    cfg, rmq_conn, ch = open_channel()
    session = SessionLocal()
    inserted = 0
    raw_ids = []
    try:
        for payload in fetch_vendor_a_products():
            raw_id = insert_raw_product(session,VENDOR_CODE, payload)
            raw_ids.append(raw_id)
            inserted += 1
        session.commit()
        for raw_id in raw_ids:
            publish_raw_ingested(ch, cfg, VENDOR_CODE, raw_id)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        rmq_conn.close()
    print(f"OK: inserted={inserted}")


if __name__ == "__main__":
    main()
