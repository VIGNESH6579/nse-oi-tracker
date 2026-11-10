import json
import logging
import re
from datetime import date, datetime
from html import unescape

from .documents import fetch_official_document, is_official_document_url

log = logging.getLogger(__name__)

FACT_ALIASES = {
    "revenue": ("RevenueFromOperations",),
    "other_income": ("OtherIncome",),
    "total_income": ("Income", "TotalIncome"),
    "pbt": ("ProfitBeforeTax",),
    "pbt_before_exceptional": ("ProfitBeforeExceptionalItemsAndTax",),
    "pat": ("ProfitLossForPeriodFromContinuingOperations", "ProfitLossForPeriod"),
    "eps": (
        "BasicEarningsLossPerShareFromContinuingOperations",
        "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    ),
    "exceptional": ("ExceptionalItemsBeforeTax", "ExceptionalItems"),
    "finance_costs": ("FinanceCosts",),
    "depreciation": ("DepreciationDepletionAndAmortisationExpense",),
}

ROUTINE_TEXT = (
    "trading window",
    "shareholding pattern",
    "newspaper publication",
    "newspaper advertisement",
    "investor presentation",
    "earnings conference call",
    "earnings call",
    "audio recording",
    "transcript of",
    "loss of share certificate",
    "reconciliation of share capital",
    "compliance certificate",
    "secretarial compliance",
    "voting results",
    "scrutinizer",
    "intimation of book closure",
)
RESULT_ANCILLARY = (
    "transcript",
    "audio recording",
    "earnings conference call",
    "earnings call recording",
    "investor presentation",
    "newspaper",
    "clarification",
)


def indian_fy_quarter(start: date, end: date) -> str | None:
    days = (end - start).days
    if days < 0:
        return None
    if start.month == 4 and end.month == 6:
        quarter = "Q1"
    elif start.month == 7 and end.month == 9:
        quarter = "Q2"
    elif start.month == 10 and end.month == 12:
        quarter = "Q3"
    elif start.month == 1 and end.month == 3:
        quarter = "Q4"
    elif start.month == 4 and end.month == 3 and days > 300:
        fy = end.year if end.month <= 3 else end.year + 1
        return f"FY{str(fy)[2:]}"
    elif 70 <= days <= 100:
        quarter = {6: "Q1", 9: "Q2", 12: "Q3", 3: "Q4"}.get(end.month)
        if not quarter:
            return None
    else:
        return None
    fy = end.year + 1 if end.month >= 4 else end.year
    return f"{quarter} FY{str(fy)[2:]}"


def _parse_display_date(raw: str) -> date | None:
    token = (raw or "").strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(token, fmt).date()
        except ValueError:
            continue
    return None


def parse_period_from_text(*parts: str) -> dict:
    text = " ".join(str(part or "") for part in parts)
    start = None
    end = None
    start_match = re.search(
        r"PERIOD START DATE\s*:\s*(\d{1,2}[-/][A-Za-z]{3,}[-/]\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{4})",
        text,
        flags=re.I,
    )
    end_match = re.search(
        r"PERIOD END DATE\s*:\s*(\d{1,2}[-/][A-Za-z]{3,}[-/]\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{4})",
        text,
        flags=re.I,
    )
    if start_match:
        start = _parse_display_date(start_match.group(1))
    if end_match:
        end = _parse_display_date(end_match.group(1))
    label = None
    statement = None
    if re.search(r"\bconsolidated\b", text, flags=re.I):
        statement = "CONSOLIDATED"
    elif re.search(r"\bstandalone\b", text, flags=re.I):
        statement = "STANDALONE"
    if start and end:
        label = indian_fy_quarter(start, end)
    if not label:
        qmatch = re.search(r"\b(Q[1-4])\s*(?:FY\s*)?(20\d{2}|\d{2})\b", text, flags=re.I)
        if qmatch:
            year = qmatch.group(2)
            if len(year) == 4:
                year = year[2:]
            label = f"{qmatch.group(1).upper()} FY{year}"
    return {
        "period_start": start.isoformat() if start else None,
        "period_end": end.isoformat() if end else None,
        "result_period": label,
        "statement_type": statement,
    }


def _parse_number(raw: str, scale: str, sign: str) -> float | None:
    text = unescape(raw or "").replace(",", "").replace(" ", "").replace("\xa0", "")
    if text in {"", "-", "—"}:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if sign == "-":
        value = -value
    try:
        value *= 10 ** int(scale or 0)
    except ValueError:
        return None
    return value


def _parse_date(raw: str) -> date | None:
    try:
        return datetime.strptime(raw.strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_ixbrl(html: str) -> dict:
    contexts = {}
    for match in re.finditer(
        r'<xbrli:context[^>]*id=["\']([^"\']+)["\'][^>]*>(.*?)</xbrli:context>',
        html,
        flags=re.I | re.S,
    ):
        cid, body = match.group(1), match.group(2)
        start = re.search(r"<xbrli:startDate>([^<]+)", body, flags=re.I)
        end = re.search(r"<xbrli:endDate>([^<]+)", body, flags=re.I)
        if not start or not end:
            continue
        start_d = _parse_date(start.group(1))
        end_d = _parse_date(end.group(1))
        if not start_d or not end_d:
            continue
        days = (end_d - start_d).days
        if days < 20:
            continue
        contexts[cid] = {
            "start": start_d,
            "end": end_d,
            "days": days,
            "kind": "quarter" if 70 <= days <= 100 else ("half" if 150 <= days <= 200 else ("annual" if days >= 300 else "other")),
        }

    facts = {}
    for match in re.finditer(r"<ix:nonFraction([^>]*)>([^<]*)</ix:nonFraction>", html, flags=re.I):
        attrs, raw = match.group(1), match.group(2)
        name_match = re.search(r'name=["\']([^"\']+)', attrs, flags=re.I)
        ctx_match = re.search(r'contextRef=["\']([^"\']+)', attrs, flags=re.I)
        scale_match = re.search(r'scale=["\']([^"\']+)', attrs, flags=re.I)
        sign_match = re.search(r'sign=["\']([^"\']+)', attrs, flags=re.I)
        if not name_match or not ctx_match:
            continue
        name = name_match.group(1).split(":")[-1]
        ctx = ctx_match.group(1)
        if ctx not in contexts:
            continue
        value = _parse_number(raw, scale_match.group(1) if scale_match else "0",
                              sign_match.group(1) if sign_match else "")
        if value is None:
            continue
        facts.setdefault(name, {})[ctx] = value

    statement = "UNKNOWN"
    nature = re.findall(
        r'name=["\'][^"\']*NatureOfReportStandaloneConsolidated["\'][^>]*>([^<]+)',
        html,
        flags=re.I,
    )
    if nature:
        token = nature[0].strip().upper()
        if "CONSOLID" in token:
            statement = "CONSOLIDATED"
        elif "STANDALONE" in token:
            statement = "STANDALONE"

    quarter_ids = [cid for cid, meta in contexts.items() if meta["kind"] == "quarter"]
    if not quarter_ids:
        return {"ok": False, "reason": "No quarterly duration context found in iXBRL", "statement_type": statement}

    current_id = max(quarter_ids, key=lambda cid: contexts[cid]["end"])
    current_meta = contexts[current_id]
    yoy_id = None
    qoq_id = None
    for cid, meta in contexts.items():
        if cid == current_id or meta["kind"] != "quarter":
            continue
        end_delta = (current_meta["end"] - meta["end"]).days
        if 350 <= end_delta <= 380:
            yoy_id = cid
        elif 80 <= end_delta <= 100:
            qoq_id = cid

    def pick(aliases, ctx_id):
        if not ctx_id:
            return None
        for alias in aliases:
            if alias in facts and ctx_id in facts[alias]:
                return facts[alias][ctx_id]
        return None

    current = {key: pick(aliases, current_id) for key, aliases in FACT_ALIASES.items()}
    yoy = {key: pick(aliases, yoy_id) for key, aliases in FACT_ALIASES.items()} if yoy_id else {}
    qoq = {key: pick(aliases, qoq_id) for key, aliases in FACT_ALIASES.items()} if qoq_id else {}

    def ebitda(row):
        pbt = row.get("pbt_before_exceptional")
        if pbt is None:
            pbt = row.get("pbt")
        finance = row.get("finance_costs")
        dep = row.get("depreciation")
        if None in (pbt, finance, dep):
            return None
        return pbt + finance + dep

    current["ebitda"] = ebitda(current)
    if yoy:
        yoy["ebitda"] = ebitda(yoy)
    if qoq:
        qoq["ebitda"] = ebitda(qoq)
    if current.get("total_income") is None and current.get("revenue") is not None:
        other = current.get("other_income") or 0
        current["total_income"] = current["revenue"] + other

    period = indian_fy_quarter(current_meta["start"], current_meta["end"])
    return {
        "ok": True,
        "reason": None,
        "statement_type": statement,
        "result_period": period,
        "period_start": current_meta["start"].isoformat(),
        "period_end": current_meta["end"].isoformat(),
        "current": current,
        "yoy": yoy,
        "qoq": qoq,
        "extraction": "ixbrl",
        "comparatives": {
            "yoy": bool(yoy_id and yoy.get("pat") is not None),
            "qoq": bool(qoq_id and qoq.get("pat") is not None),
        },
    }


def rupees_to_crore(value) -> float | None:
    if value is None:
        return None
    return round(float(value) / 10_000_000, 2)


def pct_change(current, previous) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round(((current - previous) / abs(previous)) * 100, 1)


def classify_impact(parsed: dict) -> tuple[str, str]:
    if not parsed.get("ok"):
        return "unassessed", parsed.get("reason") or (
            "Figures could not be extracted from the official document. Impact not assessed."
        )
    current = parsed.get("current") or {}
    yoy = parsed.get("yoy") or {}
    revenue_yoy = pct_change(current.get("revenue"), yoy.get("revenue"))
    pat_yoy = pct_change(current.get("pat"), yoy.get("pat"))
    ebitda_yoy = pct_change(current.get("ebitda"), yoy.get("ebitda"))
    exceptional = current.get("exceptional") or 0
    pat = current.get("pat")
    material_exceptional = pat not in (None, 0) and abs(exceptional) >= 0.1 * abs(pat)

    if revenue_yoy is None or pat_yoy is None:
        return (
            "unassessed",
            "Current-period figures were extracted, but the official iXBRL filing did not include a matching previous-year quarter for YoY comparison. Impact not assessed.",
        )
    if material_exceptional:
        return (
            "mixed",
            "Reported profit includes a material exceptional item, so operating performance cannot be judged from PAT growth alone.",
        )
    if revenue_yoy >= 8 and pat_yoy >= 10 and (ebitda_yoy is None or ebitda_yoy >= 0):
        return (
            "bullish",
            f"YoY revenue {revenue_yoy:+.1f}% and PAT {pat_yoy:+.1f}% with no material exceptional item in the extracted figures.",
        )
    if revenue_yoy <= -5 and pat_yoy <= -10:
        return (
            "bearish",
            f"YoY revenue {revenue_yoy:+.1f}% and PAT {pat_yoy:+.1f}% in the extracted figures.",
        )
    if (revenue_yoy > 0 and pat_yoy < 0) or (revenue_yoy < 0 and pat_yoy > 0):
        return (
            "mixed",
            f"Conflicting YoY signals: revenue {revenue_yoy:+.1f}% vs PAT {pat_yoy:+.1f}%.",
        )
    return (
        "unassessed",
        f"YoY revenue {revenue_yoy:+.1f}% and PAT {pat_yoy:+.1f}% are not decisive enough for a directional call.",
    )


def format_crore(value) -> str:
    crore = rupees_to_crore(value)
    if crore is None:
        return "Not disclosed in extracted filing"
    return f"₹{crore:,.2f} crore"


def analysis_from_parsed(parsed: dict, source_url: str) -> dict:
    impact, rationale = classify_impact(parsed)
    current = parsed.get("current") or {}
    yoy = parsed.get("yoy") or {}
    qoq = parsed.get("qoq") or {}
    return {
        "extraction_status": "ok" if parsed.get("ok") else "failed",
        "extraction": parsed.get("extraction") or "none",
        "result_period": parsed.get("result_period"),
        "statement_type": parsed.get("statement_type") or "UNKNOWN",
        "period_start": parsed.get("period_start"),
        "period_end": parsed.get("period_end"),
        "revenue_current_crore": rupees_to_crore(current.get("revenue")),
        "revenue_yoy_crore": rupees_to_crore(yoy.get("revenue")),
        "revenue_yoy_pct": pct_change(current.get("revenue"), yoy.get("revenue")),
        "revenue_qoq_pct": pct_change(current.get("revenue"), qoq.get("revenue")),
        "ebitda_current_crore": rupees_to_crore(current.get("ebitda")),
        "ebitda_yoy_crore": rupees_to_crore(yoy.get("ebitda")),
        "ebitda_yoy_pct": pct_change(current.get("ebitda"), yoy.get("ebitda")),
        "pat_current_crore": rupees_to_crore(current.get("pat")),
        "pat_yoy_crore": rupees_to_crore(yoy.get("pat")),
        "pat_yoy_pct": pct_change(current.get("pat"), yoy.get("pat")),
        "eps": current.get("eps"),
        "exceptional_crore": rupees_to_crore(current.get("exceptional")),
        "impact": impact,
        "rationale": rationale,
        "source_url": source_url,
        "comparatives": parsed.get("comparatives") or {},
    }


def enrich_result_event(event: dict) -> dict:
    event = dict(event)
    period_info = parse_period_from_text(event.get("headline"), event.get("details"))
    if period_info.get("result_period"):
        event["result_period"] = period_info["result_period"]
    if period_info.get("statement_type"):
        event["statement_type"] = period_info["statement_type"]
    url = str(event.get("source_url") or "")
    if not is_official_document_url(url):
        event.setdefault("analysis_json", json.dumps({
            "extraction_status": "skipped",
            "reason": "No official NSE/BSE document URL available for extraction.",
        }))
        return event
    if "xbrl" not in url.lower() and not url.lower().endswith((".html", ".htm", ".xml")):
        event["analysis_json"] = json.dumps({
            "extraction_status": "skipped",
            "reason": "Linked filing is not an iXBRL results document; numbers were not guessed from the PDF headline.",
            "source_url": url,
        })
        return event
    try:
        payload, content_type = fetch_official_document(url)
        text = payload.decode("utf-8", errors="ignore")
        if "<ix:nonFraction" not in text and "xbrli:context" not in text:
            parsed = {"ok": False, "reason": "Downloaded document did not contain iXBRL facts."}
        else:
            parsed = parse_ixbrl(text)
        analysis = analysis_from_parsed(parsed, url)
        event["analysis_json"] = json.dumps(analysis)
        event["impact"] = analysis.get("impact") or event.get("impact") or "unassessed"
        event["rationale"] = analysis.get("rationale") or event.get("rationale")
        if analysis.get("result_period"):
            event["result_period"] = analysis["result_period"]
        if analysis.get("statement_type") and analysis["statement_type"] != "UNKNOWN":
            event["statement_type"] = analysis["statement_type"]
        return event
    except Exception as exc:
        log.warning("Result document extraction failed for %s: %s", event.get("symbol"), exc)
        event["analysis_json"] = json.dumps({
            "extraction_status": "failed",
            "reason": "Official document could not be downloaded or parsed.",
            "source_url": url,
        })
        return event


def is_routine_announcement(title: str, details: str) -> bool:
    text = f"{title} {details}".lower()
    return any(token in text for token in ROUTINE_TEXT)


def is_result_ancillary(title: str, details: str) -> bool:
    text = f"{title} {details}".lower()
    return any(token in text for token in RESULT_ANCILLARY)
