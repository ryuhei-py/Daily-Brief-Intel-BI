from pathlib import Path

from src.app.ingest.estat import parse_estat
from src.app.ingest.rss import parse_rss


def test_parse_rss_sample():
    fixture = Path("tests/fixtures/rss_sample.xml").read_text(encoding="utf-8")
    source = {"id": "rss1", "name": "RSS Source", "category": "jp", "kind": "rss"}
    items = parse_rss(fixture, source)
    assert len(items) == 2
    assert items[0]["source_id"] == "rss1"
    assert items[0]["title"]
    assert items[0]["url"]


def test_parse_estat_sample():
    fixture = Path("tests/fixtures/estat_sample.json").read_text(encoding="utf-8")
    source = {
        "id": "estat1",
        "name": "eStat",
        "category": "jp",
        "kind": "estat_api",
        "url": "https://api.example.com",
    }
    items = parse_estat(fixture, source)
    assert len(items) == 2
    assert items[0]["source_id"] == "estat1"
    assert items[0]["summary"]
    assert items[0]["title"]
