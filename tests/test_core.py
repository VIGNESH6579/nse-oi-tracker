from app.nse import classify, normalize

def test_classifies_results():
    assert classify("Financial results for quarter ended", "")[0] == "Financial Results"

def test_uncertain_is_default():
    assert classify("Disclosure under regulation 30", "")[1] == "uncertain"

def test_normalize_filters_symbols():
    assert normalize({"symbol":"ABC","desc":"Board meeting","an_dt":"2026-01-01"}, {"XYZ"}) is None

def test_normalize_creates_event():
    item = normalize({"symbol":"XYZ","desc":"Board meeting","an_dt":"2026-01-01"}, {"XYZ"})
    assert item["symbol"] == "XYZ"
    assert item["category"] == "Board Meeting"
    assert len(item["event_id"]) == 64
