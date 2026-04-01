"""
Phase 1 — API Extractor
=======================
Pulls all weapon and boss data from the Elden Ring Fan REST API.

Design decisions
----------------
* Pagination: the API uses ?limit=N&page=N cursor. We fetch until `data` is
  empty or `count` < `limit` (last page indicator).
* Idempotency: raw JSON pages are written to the data lake as
  {endpoint}/{YYYY-MM-DD}/{page:04d}.json.  Re-running on the same calendar
  date overwrites the same files — no duplicates accumulate.
* Resilience: @retry with exponential back-off handles transient 429 / 5xx.
  A failed page is skipped and logged rather than aborting the entire run.
* Rate limiting: configurable REQUEST_DELAY between pages to be a polite
  API citizen.
"""

import json
import time
from datetime import date
from pathlib import Path

import requests
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.config import APIConfig, DataLakeConfig


# ── Retry decorator ──────────────────────────────────────────────────────────

@retry(
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _get(url: str, params: dict, session: requests.Session) -> dict:
    """Single GET with retry on transient network errors."""
    resp = session.get(url, params=params, timeout=15)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 10))
        logger.warning(f"Rate limited — sleeping {retry_after}s")
        time.sleep(retry_after)
        resp.raise_for_status()          # will trigger tenacity retry
    resp.raise_for_status()
    return resp.json()


# ── Core extractor ───────────────────────────────────────────────────────────

class EldenRingAPIExtractor:
    """
    Paginates through every endpoint listed in APIConfig.ENDPOINTS and
    writes raw JSON pages to the data lake.

    Usage
    -----
        extractor = EldenRingAPIExtractor()
        extractor.run()                    # extract all endpoints
        extractor.extract_endpoint("weapons")   # extract one only
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.run_date = date.today().isoformat()

    # ── Public API ──────────────────────────────────────────────────────────

    def run(self) -> dict[str, int]:
        """Extract all configured endpoints. Returns {endpoint: total_records}."""
        summary: dict[str, int] = {}
        for endpoint in APIConfig.ENDPOINTS:
            total = self.extract_endpoint(endpoint)
            summary[endpoint] = total
        logger.info(f"Extraction complete: {summary}")
        return summary

    def extract_endpoint(self, endpoint: str) -> int:
        """
        Paginate through `endpoint` and persist every page as JSON.
        Returns the total number of records extracted.
        """
        page = 0
        total_extracted = 0
        logger.info(f"Starting extraction: /{endpoint}")

        while True:
            url = f"{APIConfig.BASE_URL}/{endpoint}"
            params = {"limit": APIConfig.PAGE_SIZE, "page": page}

            try:
                payload = _get(url, params, self.session)
            except Exception as exc:
                logger.error(f"/{endpoint} page {page} failed after retries: {exc}")
                break          # skip remaining pages — partial data is better than none

            data = payload.get("data", [])
            if not data:
                break          # empty page → exhausted

            self._write_page(endpoint, page, payload)
            total_extracted += len(data)
            logger.debug(f"  /{endpoint} page {page}: {len(data)} records (total so far: {total_extracted})")

            # Last page: API count < requested page_size
            if len(data) < APIConfig.PAGE_SIZE:
                break

            page += 1
            time.sleep(APIConfig.REQUEST_DELAY)   # polite rate-limiting

        logger.info(f"/{endpoint}: extracted {total_extracted} records across {page + 1} pages")
        return total_extracted

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _write_page(self, endpoint: str, page: int, payload: dict) -> Path:
        """
        Persist one API page to the data lake.
        Path: {DATA_LAKE_ROOT}/{endpoint}/{run_date}/{page:04d}.json

        Idempotent: writing on the same date overwrites the same file path,
        so pipeline reruns do not accumulate duplicate raw files.
        """
        out_dir = DataLakeConfig.ROOT / endpoint / self.run_date
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{page:04d}.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        return out_path

    # ── Utility: load all pages for an endpoint ──────────────────────────────

    @staticmethod
    def load_from_lake(endpoint: str, run_date: str | None = None) -> list[dict]:
        """
        Re-read all pages for `endpoint` from the data lake.
        If `run_date` is None, uses the most recent date directory.
        Returns a flat list of record dicts.
        """
        base = DataLakeConfig.ROOT / endpoint
        if run_date:
            date_dir = base / run_date
        else:
            date_dirs = sorted(base.iterdir()) if base.exists() else []
            if not date_dirs:
                raise FileNotFoundError(f"No data lake files found for endpoint '{endpoint}'")
            date_dir = date_dirs[-1]

        records: list[dict] = []
        for page_file in sorted(date_dir.glob("*.json")):
            payload = json.loads(page_file.read_text())
            records.extend(payload.get("data", []))
        logger.info(f"Loaded {len(records)} records from lake for '{endpoint}' ({date_dir.name})")
        return records


if __name__ == "__main__":
    extractor = EldenRingAPIExtractor()
    extractor.run()
