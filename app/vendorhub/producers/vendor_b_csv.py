import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

from vendorhub.db.session import SessionLocal
from vendorhub.db.staging_repo import insert_raw_product
from vendorhub.transforms.vendor_b import normalize_csv

from vendorhub.mq.rabbit import open_channel, publish_raw_ingested

VENDOR_CODE = "vendor_b"


def process_one_file(path: Path, ch, cfg, archive_dir):
    df = pd.read_csv(path)
    norm = normalize_csv(df)
    session = SessionLocal()

    try:
        inserted = 0
        raw_ids = []

        for row in norm.to_dict(orient="records"):
            raw_id = insert_raw_product(session, VENDOR_CODE, row)
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

    # архивируем файл, чтобы не прогонять повторно
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived = archive_dir / f"{path.stem}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}{path.suffix}" #создание имени файла stem - корень, suffix - .окончание
    shutil.move(str(path), str(archived))  #перемещение файла откуда куда

    return inserted, archived


def main():
    cfg, rmq_conn, ch = open_channel()

    inbox_dir = cfg.inbox_dir
    archive_dir = cfg.archive_dir

    inbox_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    for csv_path in sorted(inbox_dir.glob("*.csv")):
        ins, archived = process_one_file(csv_path, ch, cfg, archive_dir)
        total += ins
        print(f"Processed {csv_path.name}: inserted={ins}, archived={archived.name}")

    rmq_conn.close()
    print(f"OK: total_inserted={total}")


if __name__ == "__main__":
    main()
