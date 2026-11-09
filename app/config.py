import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "NSE Company News Monitor")
    database_path: str = os.getenv("DATABASE_PATH", "./data/news.db")
    poll_interval: int = max(60, int(os.getenv("POLL_INTERVAL_SECONDS", "300")))
    nse_timeout: int = max(5, int(os.getenv("NSE_TIMEOUT_SECONDS", "20")))
    nse_retries: int = max(1, min(5, int(os.getenv("NSE_MAX_RETRIES", "3"))))
    universe_csv_url: str = os.getenv("FNO_UNIVERSE_CSV_URL", "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv")
    fno_symbols: frozenset[str] = frozenset(x.strip().upper() for x in os.getenv("FNO_SYMBOLS", "").split(",") if x.strip())
    ntfy_base_url: str = os.getenv("NTFY_BASE_URL", "https://ntfy.sh").rstrip("/")
    ntfy_topic: str = os.getenv("NTFY_TOPIC", "").strip()
    ntfy_token: str = os.getenv("NTFY_TOKEN", "").strip()
    angel_market_data_enabled: bool = os.getenv("ANGEL_MARKET_DATA_ENABLED", "1").strip().lower() in {"1","true","yes","on"}

settings = Settings()
