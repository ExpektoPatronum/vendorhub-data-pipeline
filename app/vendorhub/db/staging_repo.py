import json
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session
from typing import Any, Dict, Tuple, List



def insert_raw_product(session: Session, vendor_code: str, payload: dict) -> int:
    row = session.execute(
        text(
            """
            insert into staging.vendor_products_raw (vendor_code, payload)
            values (:vendor_code, CAST(:payload AS jsonb))
            returning id
            """
        ),
        {"vendor_code": vendor_code, "payload": json.dumps(payload)},
    ).fetchone()

    return int(row[0])

def read_raw_payload(session: Session, raw_id: int) -> Tuple[Dict[str, Any], str]:
    # Через SQLAlchemy text() читаем jsonb payload
    row = session.execute(
        text("select payload, vendor_code from staging.vendor_products_raw where id = :id"),
        {"id": raw_id},
    ).fetchone()

    if not row:
        raise RuntimeError(f"raw_id={raw_id} not found in staging")

    data = row._mapping  #превращаем тип row в rowmapper который ведет себя как обычный dict (надежно)
    payload = data["payload"]
    vendor_code = data["vendor_code"]
    return payload, vendor_code

def insert_rejected_rows(session: Session, rejected_payload: List[Dict[str, Any]]) -> None:
    session.execute(
        text("""
             insert into staging.rejected_rows
                 (raw_id, vendor_code, error_reasons, payload)
             values (:raw_id, :vendor_code, cast(:error_reason as jsonb), cast(:payload as jsonb))
             """),
        rejected_payload,
    )

def fetch_raw_rows(session, raw_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Одна выборка пачки raw по ids.
    """
    if not raw_ids:
        return []

    stmt = text(
        """
        select id as raw_id, vendor_code, payload
        from staging.vendor_products_raw
        where id in :ids
        """
    ).bindparams(bindparam("ids", expanding=True))  #bindparams нжуен чтоб передать массив значений в sql запрос а expanding=True раскрывает переданный масив иначе никак при работе с IN в запросе

    rows = session.execute(stmt, {"ids": raw_ids}).mappings().all() # sql запрос возвращает row objects, mappings() делает из них dict-like объекты, но result это все еще итератор и чтоб сразу все получить мы пишем .all и получаем список словарей
    return [dict(r) for r in rows]