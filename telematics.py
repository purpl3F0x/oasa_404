import asyncio
import csv
import json
import logging
import os
import pathlib
import random
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from git import Repo
from tqdm import tqdm

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

BASE_URL = "http://telematics.oasa.gr/api/"

COLUMNS = [
    "line_circle",
    "line_descr",
    "line_descr_eng",
    "line_id",
    "remarks",
    "sdc_code",
    "sdd_aa",
    "sdd_code",
    "sdd_kp1",
    "sdd_kp2",
    "sdd_line1",
    "sdd_line2",
    "sdd_sort",
    "sdd_start1",
    "sde_end1",
    "sde_end2",
    "sde_start1",
    "sde_start2",
    "sds_code",
]

# ------------------------------
# Blocking HTTP using urllib (kept)
# ------------------------------


def _blocking_post(payload: bytes, timeout: float = 15.0) -> Any:
    req = urllib.request.Request(
        BASE_URL,
        data=payload,
        headers={
            "User-Agent": "oasa-scraper/1.0 (+asyncio)",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw)


def urlencode(d: Dict[str, str]) -> bytes:
    return urllib.parse.urlencode(d).encode()


# ------------------------------
# CSV + filenames
# ------------------------------


def safe_filename(s: str) -> str:
    if s is None:
        return "unknown"
    return "".join(
        c if (c.isalnum() or c in (" ", "-", "_", ".", "·")) else "_" for c in str(s)
    ).strip()


def to_csv_fixed_rows(
    rows: List[Dict[str, Any]], out_path: pathlib.Path, *, excel_friendly=True
):
    enc = "utf-8-sig" if excel_friendly else "utf-8"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w+", encoding=enc, newline="") as f:
        # f.truncate(0)
        # f.seek(0)
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            cleaned = {k: ("" if row.get(k) is None else row.get(k)) for k in COLUMNS}
            w.writerow(cleaned)


# ------------------------------
# Adaptive concurrency limiter (AIMD)
# ------------------------------


class AdaptiveLimiter:
    def __init__(
        self,
        *,
        initial: int = 4,
        min_limit: int = 2,
        max_limit: int = 64,
        inc_every: int = 20,
        fast_latency_s: float = 0.7,
        decrease_factor: float = 0.7,
    ):
        self.limit = initial
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.inc_every = inc_every
        self.fast_latency_s = fast_latency_s
        self.decrease_factor = decrease_factor

        self._in_flight = 0
        self._success_streak = 0
        self._cond = asyncio.Condition()

    async def acquire(self):
        async with self._cond:
            while self._in_flight >= self.limit:
                await self._cond.wait()
            self._in_flight += 1

    async def release(self):
        async with self._cond:
            self._in_flight -= 1
            self._cond.notify_all()

    async def record(self, success: bool, latency_s: float):
        if success and latency_s <= self.fast_latency_s:
            self._success_streak += 1
            if self._success_streak >= self.inc_every and self.limit < self.max_limit:
                self.limit += 1
                self._success_streak = 0
                logger.debug("Increasing concurrency -> %d", self.limit)
        elif not success:
            new_limit = max(
                self.min_limit, max(1, int(self.limit * self.decrease_factor))
            )
            if new_limit < self.limit:
                self.limit = new_limit
                logger.warning("Decreasing concurrency -> %d", self.limit)
            self._success_streak = 0
        else:
            self._success_streak = 0


# ------------------------------
# Async HTTP wrapper on threadpool
# ------------------------------


class AsyncHTTP:
    def __init__(self, executor: ThreadPoolExecutor, limiter: AdaptiveLimiter):
        self._executor = executor
        self._limiter = limiter

    async def post(
        self, params: Dict[str, str], timeout: float = 15.0, retries: int = 3
    ) -> Any:
        """
        Async wrapper with retries + exponential backoff.
        """
        payload = urlencode(params)
        loop = asyncio.get_running_loop()

        for attempt in range(1, retries + 1):
            await self._limiter.acquire()
            t0 = time.monotonic()
            try:
                data = await loop.run_in_executor(
                    self._executor, _blocking_post, payload, timeout
                )
                latency = time.monotonic() - t0
                await self._limiter.record(True, latency)
                return data
            except Exception as e:
                latency = time.monotonic() - t0
                await self._limiter.record(False, latency)
                logger.warning(
                    "Request failed (%s) attempt %d/%d: %s",
                    params.get("act", ""),
                    attempt,
                    retries,
                    e,
                )
                if attempt < retries:
                    # exponential backoff with jitter
                    delay = (2 ** (attempt - 1)) + random.random()
                    await asyncio.sleep(delay)
                else:
                    raise
            finally:
                await self._limiter.release()


# ------------------------------
# API endpoints (async)
# ------------------------------


async def webGetLinesWithMLInfo(http: AsyncHTTP):
    return await http.post({"act": "webGetLinesWithMLInfo"})


async def getDailySchedule(http: AsyncHTTP, line_code: str):
    return await http.post({"act": "getDailySchedule", "line_code": line_code})


async def getScheduleDaysMasterline(http: AsyncHTTP, line_code: str):
    return await http.post({"act": "getScheduleDaysMasterline", "p1": line_code})


async def getSchedLines(http: AsyncHTTP, ml_code: str, sdc_code: str, line_code: str):
    return await http.post(
        {"act": "getSchedLines", "p1": ml_code, "p2": sdc_code, "p3": line_code}
    )


# ------------------------------
# Orchestration
# ------------------------------


def current_service_folder(tz: str = "Europe/Athens") -> str:
    now = datetime.now(ZoneInfo(tz))
    wd = now.weekday()  # Monday=0 ... Sunday=6
    if wd == 5:
        return "saturday"
    if wd == 6:
        return "sunday"
    return "daily"


async def main(output_root: pathlib.Path = pathlib.Path("./db")):
    # Big pool; real concurrency governed by AdaptiveLimiter
    executor = ThreadPoolExecutor(max_workers=64, thread_name_prefix="oasa")
    limiter = AdaptiveLimiter(
        initial=6, min_limit=2, max_limit=100, inc_every=30, fast_latency_s=0.6
    )
    http = AsyncHTTP(executor, limiter)

    # Stage 0: lines
    lines = await webGetLinesWithMLInfo(http)
    if not isinstance(lines, list):
        raise RuntimeError("Unexpected response for webGetLinesWithMLInfo")

    masters = [e for e in lines if e.get("mld_master") != "0"]
    pbar_lines = tqdm(total=len(masters), desc="Lines", unit="line", colour="blue")

    # Stage 1: fetch days concurrently
    async def fetch_days(
        entry: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[List[Dict[str, Any]]]]:
        try:
            days = await getScheduleDaysMasterline(http, entry["line_code"])
            return entry, days or []
        except Exception as e:
            logger.error(
                "getScheduleDaysMasterline failed for line %s: %s",
                entry.get("line_id"),
                e,
            )
            return entry, []
        finally:
            pbar_lines.update(1)

    days_results = await asyncio.gather(*(fetch_days(e) for e in masters))

    total_days = sum(len(days) for _, days in days_results)
    pbar_days = tqdm(
        total=total_days, desc="Timetables", unit="tables", colour="magenta"
    )

    daily_folder = current_service_folder()
    pbar_daily = tqdm(
        total=len(masters), desc="Daily schedules", unit="line", colour="green"
    )

    # Stage 2: Fetch daily schedules and write CSVs
    async def fetch_and_write_daily(entry: Dict[str, Any]):
        try:
            sch = await getDailySchedule(http, entry["line_code"])
        except Exception as e:
            logger.error(
                "getDailySchedule failed for line %s : %s",
                entry.get("line_id"),
                e,
            )
            pbar_days.update(1)
            return

        write_jobs = []
        for k, v in sch.items():
            if not v:
                continue
            try:
                line_id = (entry.get("line_id") or "").strip()
                filename = pathlib.Path(f"./db/{line_id}/{daily_folder}/{k}.csv")
                write_jobs.append(
                    asyncio.to_thread(
                        to_csv_fixed_rows, v, filename, excel_friendly=True
                    )
                )
            except Exception as e:
                logger.error(
                    "Prep write failed for %s / %s / %s: %s", line_id, day, k, e
                )

        if write_jobs:
            # write concurrently off the event loop
            await asyncio.gather(*write_jobs)

        pbar_daily.update(1)

    stage2 = []
    for entry in lines:
        stage2.append(fetch_and_write_daily(entry))

    if stage2:
        await asyncio.gather(*stage2)

    # Stage 3: for each day fetch schedules and write CSVs
    async def process_line_day(entry: Dict[str, Any], day: Dict[str, Any]):
        try:
            sch = await getSchedLines(
                http, entry["ml_code"], day["sdc_code"], entry["line_code"]
            )
        except Exception as e:
            logger.error(
                "getSchedLines failed for line %s / day %s: %s",
                entry.get("line_id"),
                day.get("sdc_code"),
                e,
            )
            pbar_days.update(1)
            return

        write_jobs = []
        for k, v in sch.items():
            if not v:
                continue
            try:
                line_id = (entry.get("line_id") or "").strip()
                day_dir = safe_filename(day.get("sdc_descr_eng", day.get("sdc_code")))
                filename = pathlib.Path(f"./db/{line_id}/{day_dir}/{k}.csv")
                write_jobs.append(
                    asyncio.to_thread(
                        to_csv_fixed_rows, v, filename, excel_friendly=True
                    )
                )
            except Exception as e:
                logger.error(
                    "Prep write failed for %s / %s / %s: %s", line_id, day_dir, k, e
                )

        if write_jobs:
            # write concurrently off the event loop
            await asyncio.gather(*write_jobs)

        pbar_days.update(1)

    stage3 = []
    for entry, days in days_results:
        for d in days:
            stage3.append(process_line_day(entry, d))

    if stage3:
        await asyncio.gather(*stage3)

    pbar_lines.close()
    pbar_days.close()
    pbar_daily.close()  # ### ADDED
    logger.info("Done. Output under: %s", output_root.resolve())


if __name__ == "__main__":
    # Run fetch task
    asyncio.run(main())

    # Commit to git
    athens_time = datetime.now(ZoneInfo("Europe/Athens"))
    athens_time_str = athens_time.strftime("%d-%m-%Y %H:%M")

    curdir = os.path.dirname(__file__)
    repo = Repo(curdir)

    # Get lists of changed and untracked files
    changed = [item.a_path for item in repo.index.diff(None)]
    untracked = repo.untracked_files
    db_changed = [
        f for f in changed + untracked if pathlib.Path(f).is_relative_to("db")
    ]

    repo.index.add(db_changed)
    if db_changed:
        repo.index.commit(athens_time_str)

        logger.info(f"Files Changes:")
        logger.info(db_changed)

        origin = repo.remote(name="origin")
        origin.push()
    else:
        logger.info("Nothing changed")
