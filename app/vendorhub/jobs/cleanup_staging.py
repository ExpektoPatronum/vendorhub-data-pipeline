import os
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from vendorhub.db.session import SessionLocal

RETENTION_DAYS = int(os.getenv("STAGING_RETENTION_DAYS", "30"))

def main():
    print(f"[{datetime.now(timezone.utc)}] Starting staging cleanup...")

    # порог: всё что старше этого времени — удаляем
    threshold = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)

    session = SessionLocal()
    try:
        result = session.execute(
            text("""
                DELETE FROM staging.vendor_products_raw
                WHERE load_ts < :threshold
            """),
            {"threshold": threshold},
        )
        session.commit()
        print(f"[{datetime.now(timezone.utc)}] Cleanup finished. Deleted rows: {result.rowcount}")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()