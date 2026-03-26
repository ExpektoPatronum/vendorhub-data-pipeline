from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from vendorhub.config import load_config

cfg = load_config()

DATABASE_URL = (
    f"postgresql+psycopg2://{cfg.pg.user}:{cfg.pg.password}"
    f"@{cfg.pg.host}:{cfg.pg.port}/{cfg.pg.db}"
)

ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, future=True)