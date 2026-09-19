"""Safe local Angel One login check; never prints credentials or tokens."""
from __future__ import annotations

import json

from integrations.angel_one_market_data import AngelLoginError, AngelOneMarketData, AngelUnavailable


def main() -> int:
    client = AngelOneMarketData.from_environment()
    if client is None:
        print(json.dumps({"status": "not_configured", "errorcode": "", "message": "required environment variables are missing"}))
        return 2
    try:
        client._login()
    except AngelLoginError as exc:
        print(json.dumps({"status": "rejected", "errorcode": exc.errorcode, "message": exc.message}))
        return 1
    except AngelUnavailable as exc:
        print(json.dumps({"status": "unavailable", "errorcode": client.health().get("last_error_code", ""), "message": str(exc)}))
        return 1
    except Exception as exc:
        print(json.dumps({"status": "error", "errorcode": type(exc).__name__, "message": "login check failed"}))
        return 1
    print(json.dumps({"status": "ok", "errorcode": "", "message": "login succeeded"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
