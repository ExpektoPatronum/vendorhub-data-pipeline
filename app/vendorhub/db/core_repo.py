from typing import List, Any, Dict, Tuple, Decimal

from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session

# ---------- DB helpers ----------
def load_vendor_map(session) -> Dict[str, int]:
    rows = session.execute(text("select vendor_code, vendor_id from core.d_vendor")).fetchall()
    return {r[0]: int(r[1]) for r in rows}

def load_currency_map(session) -> Dict[str, int]:
    rows = session.execute(text("select currency_code, currency_id from core.d_currency")).fetchall()
    return {r[0]: int(r[1]) for r in rows}

def fetch_existing_prices(session: Session, vendor_id: int, vendor_skus: List[str]) -> Dict[str, Tuple[int, Decimal | None]]:
    """
    Возвращает мапу vendor_sku -> (product_id, price)
    """
    if not vendor_skus:
        return {}

    stmt = text(
        """
        select vendor_sku, product_id, price
        from core.products
        where vendor_id = :vendor_id and vendor_sku in :skus
        """
    ).bindparams(bindparam("skus", expanding=True)) # expanding=True раскрывает список vendor_skus, для vendor_id он не нужен

    rows = session.execute(stmt, {"vendor_id": vendor_id, "skus": vendor_skus}).fetchall()
    out = {}
    for sku, pid, price in rows:
        out[str(sku)] = (int(pid), price)  # price обычно Decimal (если numeric)
    return out


def insert_price_history_batch(session, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    session.execute(
        text(
            """
            insert into core.product_price_history (product_id, old_price, new_price, raw_id, vendor_code)
            values (:product_id, :old_price, :new_price, :raw_id, :vendor_code)
            """
        ),
        rows,
    )


def upsert_products_batch(session: Session, vendor_id: int, df) -> None:
    """
    Batch upsert через executemany (SQLAlchemy сам сделает пачку параметров).
    Для портфолио это ок. Если захочешь быстрее — сделаем temp table + merge.
    """
    payloads = []
    for r in df.to_dict(orient="records"):
        payloads.append(
            {
                "vendor_id": vendor_id,
                "vendor_sku": str(r["vendor_sku"]),
                "title": r.get("title"),
                "category": r.get("category"),
                "price": Decimal(str(r["price"])) if r.get("price") is not None else None,
                "currency": r.get("currency"),
            }
        )

    session.execute(
        text(
            """
            insert into core.products (vendor_id, vendor_sku, title, category, price, currency_code, updated_at)
            values (:vendor_id, :vendor_sku, :title, :category, :price, :currency, now())
            on conflict (vendor_id, vendor_sku)
            do update set
                title = excluded.title,
                category = excluded.category,
                price = excluded.price,
                currency_code = excluded.currency_code,
                updated_at = now()
            """
        ),
        payloads,
    )